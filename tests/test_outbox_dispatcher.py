from unittest.mock import Mock, patch
from types import SimpleNamespace

import pytest

from app.queue.redis_stream import RedisStream
from app.queue.outbox_dispatcher import OutboxDispatcher


def test_dispatcher_uses_custom_configuration():
    redis_stream = Mock(spec=RedisStream)

    dispatcher = OutboxDispatcher(
        redis_stream,
        poll_interval=2.0,
        batch_size=50,
        claim_duration_seconds=120,
    )

    assert dispatcher.poll_interval == 2.0
    assert dispatcher.batch_size == 50
    assert dispatcher.claim_duration_seconds == 120
    assert dispatcher.dispatcher_id


def test_dispatcher_rejects_invalid_poll_interval():
    redis_stream = Mock(spec=RedisStream)

    with pytest.raises(ValueError, match="poll_interval"):
        OutboxDispatcher(redis_stream, poll_interval=0)


def test_dispatcher_rejects_invalid_batch_size():
    redis_stream = Mock(spec=RedisStream)

    with pytest.raises(ValueError, match="batch_size"):
        OutboxDispatcher(redis_stream, batch_size=0)


def test_dispatcher_rejects_invalid_claim_duration():
    redis_stream = Mock(spec=RedisStream)

    with pytest.raises(ValueError, match="claim_duration_seconds"):
        OutboxDispatcher(redis_stream, claim_duration_seconds=0)


def test_claim_events_commits_and_returns_event_data():
    event = SimpleNamespace(
        id="event-1",
        event_type="job.retry_scheduled",
        aggregate_id="job-1",
        payload={
            "job_id": "job-1",
            "retry_at": "2026-11-24T12:00:00+00:00",
        },
    )

    with patch("app.queue.outbox_dispatcher.SessionLocal") as session_factory:
        with patch("app.queue.outbox_dispatcher.OutboxRepository") as repository_class:
            db = session_factory.return_value

            repository = repository_class.return_value
            repository.claim_events.return_value = [event]

            dispatcher = OutboxDispatcher(redis_stream=Mock(spec=RedisStream))

            result = dispatcher.claim_events()

    assert result == [
        {
            "id": "event-1",
            "event_type": "job.retry_scheduled",
            "aggregate_id": "job-1",
            "payload": {
                "job_id": "job-1",
                "retry_at": "2026-11-24T12:00:00+00:00",
            },
        }
    ]

    repository.claim_events.assert_called_once()
    db.commit.assert_called_once()
    db.close.assert_called_once()


def test_claim_events_rolls_back_when_repository_fails():
    with patch("app.queue.outbox_dispatcher.SessionLocal") as session_factory:
        with patch("app.queue.outbox_dispatcher.OutboxRepository") as repository_class:
            db = session_factory.return_value

            repository = repository_class.return_value
            repository.claim_events.side_effect = RuntimeError("Database error")

            dispatcher = OutboxDispatcher(redis_stream=Mock(spec=RedisStream))

            with pytest.raises(RuntimeError, match="Database error"):
                dispatcher.claim_events()

    db.rollback.assert_called_once()
    db.close.assert_called_once()
    db.commit.assert_not_called()


def test_process_event_publishes_and_marks_event():
    event = {
        "id": "event-1",
        "event_type": "job.retry_scheduled",
        "aggregate_id": "job-1",
        "payload": {
            "job_id": "job-1",
            "retry_at": "2026-11-24T12:00:00+00:00",
        },
    }

    redis_stream = Mock(spec=RedisStream)

    with patch("app.queue.outbox_dispatcher.SessionLocal") as session_factory:
        with patch("app.queue.outbox_dispatcher.OutboxRepository") as repository_class:
            db = session_factory.return_value
            repository = repository_class.return_value
            repository.mark_published.return_value = True
            repository.renew_claim.return_value = True

            dispatcher = OutboxDispatcher(redis_stream=redis_stream)

            dispatcher.process_event(event)

    repository.renew_claim.assert_called_once_with(
        event_id="event-1",
        dispatcher_id=dispatcher.dispatcher_id,
        claim_duration_seconds=60,
    )

    redis_stream.schedule_delayed_job.assert_called_once()

    call_kwargs = redis_stream.schedule_delayed_job.call_args.kwargs

    assert call_kwargs["job_id"] == "job-1"
    assert call_kwargs["retry_at"].isoformat() == ("2026-11-24T12:00:00+00:00")

    repository.mark_published.assert_called_once_with(
        event_id="event-1",
        dispatcher_id=dispatcher.dispatcher_id,
    )

    assert db.commit.call_count == 2
    assert db.close.call_count == 2


def test_process_event_records_failure_when_redis_publish_fails():
    event = {
        "id": "event-1",
        "event_type": "job.retry_scheduled",
        "aggregate_id": "job-1",
        "payload": {
            "job_id": "job-1",
            "retry_at": "2026-11-24T12:00:00+00:00",
        },
    }

    redis_stream = Mock(spec=RedisStream)

    redis_stream.schedule_delayed_job.side_effect = RuntimeError("Redis unavailable")

    with patch("app.queue.outbox_dispatcher.SessionLocal") as session_factory:
        with patch("app.queue.outbox_dispatcher.OutboxRepository") as repository_class:
            db = session_factory.return_value
            repository = repository_class.return_value
            repository.renew_claim.return_value = True

            dispatcher = OutboxDispatcher(redis_stream=redis_stream)

            dispatcher.process_event(event)

    repository.renew_claim.assert_called_once()

    repository.mark_failed.assert_called_once()

    call_kwargs = repository.mark_failed.call_args.kwargs

    assert call_kwargs["event_id"] == "event-1"
    assert call_kwargs["dispatcher_id"] == dispatcher.dispatcher_id
    assert "Redis unavailable" in call_kwargs["error"]

    repository.mark_published.assert_not_called()

    assert db.commit.call_count == 2
    assert db.close.call_count == 2


def test_dispatch_once_processes_all_claimed_events():
    events = [
        {"id": "event-1"},
        {"id": "event-2"},
        {"id": "event-3"},
    ]

    dispatcher = OutboxDispatcher(redis_stream=Mock(spec=RedisStream))

    dispatcher.claim_events = Mock(return_value=events)
    dispatcher.process_event = Mock()

    result = dispatcher.dispatch_once()

    assert result == 3

    dispatcher.claim_events.assert_called_once()

    assert dispatcher.process_event.call_count == 3

    dispatcher.process_event.assert_any_call(events[0])
    dispatcher.process_event.assert_any_call(events[1])
    dispatcher.process_event.assert_any_call(events[2])


def test_dispatch_once_returns_zero_when_no_events():
    dispatcher = OutboxDispatcher(redis_stream=Mock(spec=RedisStream))

    dispatcher.claim_events = Mock(return_value=[])
    dispatcher.process_event = Mock()

    result = dispatcher.dispatch_once()

    assert result == 0

    dispatcher.claim_events.assert_called_once()
    dispatcher.process_event.assert_not_called()
