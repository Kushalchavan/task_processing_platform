import os

from dotenv import load_dotenv
from redis import Redis

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is not configured")


# Redis client manages connections through an internal connection pool.
redis_client = Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=None,
    socket_connect_timeout=5,
    health_check_interval=30,
)


def check_redis_connection() -> bool:
    return redis_client.ping()
