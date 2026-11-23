from unittest.mock import Mock

import pytest

from app.queue.redis_stream import RedisStream
from app.queue.delayed_job_dispatcher import DelayedJobDispatcher


def test_dispatch_once():
    redis_stream = Mock(spec=RedisStream)

    # Simulate Redis successfully dispatching three jobs.
    redis_stream.dispatch_due_jobs.return_value = 3

    dispatcher = DelayedJobDispatcher(redis_stream)

    result = dispatcher.dispatch_once()

    assert result == 3

    # Verify that the dispatcher requested a batch of 100 jobs.
    redis_stream.dispatch_due_jobs.assert_called_once()

    call_kwargs = redis_stream.dispatch_due_jobs.call_args.kwargs

    assert call_kwargs["count"] == 100
    assert call_kwargs["current_time"].tzinfo is not None


def test_dispatch_once_when_no_jobs_are_due():
    redis_stream = Mock(spec=RedisStream)

    redis_stream.dispatch_due_jobs.return_value = 0

    dispatcher = DelayedJobDispatcher(redis_stream)

    result = dispatcher.dispatch_once()

    assert result == 0
    redis_stream.dispatch_due_jobs.assert_called_once()


def test_dispatcher_uses_custom_configuration():
    redis_stream = Mock(spec=RedisStream)

    dispatcher = DelayedJobDispatcher(
        redis_stream,
        poll_interval=2.0,
        batch_size=50,
    )

    assert dispatcher.poll_interval == 2.0
    assert dispatcher.batch_size == 50


def test_dispatcher_rejects_invalid_poll_interval():
    redis_stream = Mock(spec=RedisStream)

    with pytest.raises(ValueError, match="poll_interval"):
        DelayedJobDispatcher(
            redis_stream,
            poll_interval=0,
        )


def test_dispatcher_rejects_invalid_batch_size():
    redis_stream = Mock(spec=RedisStream)

    with pytest.raises(ValueError, match="batch_size"):
        DelayedJobDispatcher(
            redis_stream,
            batch_size=0,
        )
