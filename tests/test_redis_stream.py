import pytest

from datetime import datetime, timedelta, timezone

from app.queue.redis_stream import RedisStream, STREAM_NAME, DELAYED_JOBS_KEY


@pytest.fixture
def redis_stream(clean_test_redis):
    stream = RedisStream(clean_test_redis)

    # Start with a fresh stream and consumer group.
    stream.create_consumer_group()

    yield stream


def test_publish_job(redis_stream):
    message_id = redis_stream.publish_job("job-123")

    assert message_id is not None


def test_consume_job(redis_stream):
    redis_stream.publish_job("job-123")

    messages = redis_stream.consume_job("worker-1")

    assert len(messages) == 1

    stream_name, entries = messages[0]
    message_id, data = entries[0]

    assert stream_name == STREAM_NAME
    assert data["job_id"] == "job-123"


def test_acknowledge_job(redis_stream):
    redis_stream.publish_job("job-123")

    messages = redis_stream.consume_job("worker-1")
    message_id = messages[0][1][0][0]

    acknowledged = redis_stream.acknowledge_job(message_id)

    assert acknowledged == 1


def test_claim_pending_messages(redis_stream):
    # Worker 1 publishes and consumes a job.
    job_id = "job-recovery-123"
    redis_stream.publish_job(job_id)

    messages = redis_stream.consume_job("worker-1")
    message_id, data = messages[0][1][0]

    # Worker 2 claims the pending message.
    result = redis_stream.claim_pending_messages(
        consumer_name="worker-2",
        min_idle_time=0,
        start_id="0-0",
        count=10,
    )

    next_start_id, claimed_messages, deleted_ids = result

    assert len(claimed_messages) == 1

    claimed_message_id, claimed_data = claimed_messages[0]

    assert claimed_message_id == message_id
    assert claimed_data["job_id"] == job_id


def test_claim_one_pending(redis_stream):
    job_id = "job-recovery-123"

    redis_stream.publish_job(job_id)
    original_messages = redis_stream.consume_job("worker-1")
    original_message_id = original_messages[0][1][0][0]

    message = redis_stream.claim_one_pending(
        consumer_name="worker-2",
        min_idle_time=0,
    )

    assert message is not None
    assert message["message_id"] == original_message_id
    assert message["job_id"] == job_id


def test_claim_one_pending_returns_none(redis_stream):
    message = redis_stream.claim_one_pending(
        consumer_name="worker-2",
        min_idle_time=0,
    )

    assert message is None


def test_schedule_delayed_job(redis_stream):
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=10)

    result = redis_stream.schedule_delayed_job(
        job_id="job-101",
        retry_at=retry_at,
    )

    # ZADD returns 1 when a new member is added.
    assert result == 1

    # Verify the job was stored in the Sorted Set.
    score = redis_stream.redis.zscore(DELAYED_JOBS_KEY, "job-101")

    assert score == pytest.approx(retry_at.timestamp())


def test_get_due_delayed_jobs(redis_stream):
    now = datetime.now(timezone.utc)

    # Schedule one job in the past and another in the future.
    redis_stream.schedule_delayed_job(
        "job-due",
        now - timedelta(seconds=5),
    )

    redis_stream.schedule_delayed_job(
        "job-future",
        now + timedelta(seconds=30),
    )

    due_jobs = redis_stream.get_due_delayed_jobs(
        current_time=now,
    )

    # Only jobs whose scheduled time has arrived should be returned.
    assert due_jobs == ["job-due"]


def test_get_due_delayed_jobs_respects_count(redis_stream):
    now = datetime.now(timezone.utc)

    # Add three jobs that are already due.
    for job_id in ["job-1", "job-2", "job-3"]:
        redis_stream.schedule_delayed_job(
            job_id,
            now - timedelta(seconds=5),
        )

    due_jobs = redis_stream.get_due_delayed_jobs(
        current_time=now,
        count=2,
    )

    assert len(due_jobs) == 2


def test_remove_delayed_job(redis_stream):
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=10)

    redis_stream.schedule_delayed_job(
        "job-101",
        retry_at,
    )

    removed = redis_stream.remove_delayed_job("job-101")

    assert removed == 1

    # Verify that the job no longer exists in the Sorted Set.
    score = redis_stream.redis.zscore(DELAYED_JOBS_KEY, "job-101")

    assert score is None


def test_schedule_delayed_job_rejects_naive_datetime(redis_stream):
    # A naive datetime has no timezone information.
    retry_at = datetime.now()

    with pytest.raises(ValueError, match="timezone-aware"):
        redis_stream.schedule_delayed_job(
            "job-101",
            retry_at,
        )


def test_dispatch_due_jobs_returns_zero_when_no_jobs_are_due(redis_stream):
    now = datetime.now(timezone.utc)

    # Schedule a job in the future.
    redis_stream.schedule_delayed_job(
        "job-future",
        now + timedelta(seconds=30),
    )

    dispatched = redis_stream.dispatch_due_jobs(
        current_time=now,
    )

    assert dispatched == 0


def test_dispatch_due_job_publishes_to_stream(redis_stream):
    now = datetime.now(timezone.utc)

    redis_stream.schedule_delayed_job(
        "job-101",
        now - timedelta(seconds=5),
    )

    dispatched = redis_stream.dispatch_due_jobs(
        current_time=now,
    )

    assert dispatched == 1

    # Verify that the job was published to the Redis Stream.
    messages = redis_stream.consume_job("worker-1")

    assert len(messages) == 1

    _, entries = messages[0]
    _, data = entries[0]

    assert data["job_id"] == "job-101"

    # Verify that the dispatched job was removed from the Sorted Set.
    assert (
        redis_stream.redis.zscore(
            DELAYED_JOBS_KEY,
            "job-101",
        )
        is None
    )


def test_dispatch_multiple_due_jobs(redis_stream):
    now = datetime.now(timezone.utc)

    # Schedule three jobs that are already due.
    for job_id, seconds_ago in [
        ("job-101", 10),
        ("job-102", 5),
        ("job-103", 2),
    ]:
        redis_stream.schedule_delayed_job(
            job_id,
            now - timedelta(seconds=seconds_ago),
        )

    dispatched = redis_stream.dispatch_due_jobs(
        current_time=now,
    )

    assert dispatched == 3

    # All three jobs should now be available in the Stream.
    messages = redis_stream.consume_job("worker-1")

    assert len(messages) == 1

    _, entries = messages[0]

    assert len(entries) == 1

    # Consume the remaining messages.
    remaining = redis_stream.redis.xlen(STREAM_NAME)

    assert remaining == 3

    # All dispatched jobs should be removed from the Sorted Set.
    assert redis_stream.redis.zcard(DELAYED_JOBS_KEY) == 0


def test_dispatch_due_jobs_respects_batch_limit(redis_stream):
    now = datetime.now(timezone.utc)

    # Schedule three due jobs.
    for job_id in ["job-101", "job-102", "job-103"]:
        redis_stream.schedule_delayed_job(
            job_id,
            now - timedelta(seconds=5),
        )

    dispatched = redis_stream.dispatch_due_jobs(
        current_time=now,
        count=2,
    )

    assert dispatched == 2

    # One job must remain scheduled for the next dispatcher cycle.
    assert redis_stream.redis.zcard(DELAYED_JOBS_KEY) == 1
    assert redis_stream.redis.xlen(STREAM_NAME) == 2
