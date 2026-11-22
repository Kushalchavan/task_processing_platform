from app.database.redis import redis_client


def test_redis_connection():
    assert redis_client.ping() is True


def test_redis_set_and_get():
    redis_client.set("nexus:test", "Hello from Python")

    result = redis_client.get("nexus:test")

    assert result == "Hello from Python"

    redis_client.delete("nexus:test")
