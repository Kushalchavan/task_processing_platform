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
