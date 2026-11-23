import pytest

from app.queue.redis_stream import RedisStream, STREAM_NAME


@pytest.fixture
def redis_stream():
    stream = RedisStream()
    stream.redis.delete(STREAM_NAME)
    stream.create_consumer_group()

    yield stream

    stream.redis.delete(STREAM_NAME)


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
