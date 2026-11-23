from typing import cast, Any

from app.database.redis import redis_client

STREAM_NAME = "nexus:jobs"
CONSUMER_GROUP = "nexus-workers"


class RedisStream:
    def __init__(self) -> None:
        self.redis = redis_client

    def create_consumer_group(self) -> None:
        try:
            self.redis.xgroup_create(
                name=STREAM_NAME,
                groupname=CONSUMER_GROUP,
                id="0",
                mkstream=True,
            )
        except Exception as exc:
            # Ignore only the error raised when the group already exists.
            if "BUSYGROUP" not in str(exc):
                raise

    def publish_job(self, job_id: str) -> str:
        message_id = self.redis.xadd(
            STREAM_NAME,
            {"job_id": job_id},
        )
        return cast(str, message_id)

    def consume_job(self, consumer_name: str) -> Any:
        return self.redis.xreadgroup(
            groupname=CONSUMER_GROUP,
            consumername=consumer_name,
            streams={STREAM_NAME: ">"},
            count=1,
            block=5000,
        )

    def acknowledge_job(self, message_id: str) -> int:
        return self.redis.xack(
            STREAM_NAME,
            CONSUMER_GROUP,
            message_id,
        )

    def consume_one(self, consumer_name: str) -> dict[str, str] | None:
        messages = self.consume_job(consumer_name)

        # No new message arrived before the timeout.
        if not messages:
            return None

        _, entries = messages[0]

        if not entries:
            return None

        message_id, data = entries[0]
        job_id = data.get("job_id")

        if not job_id:
            raise ValueError("Redis message is missing job_id")

        return {
            "message_id": message_id,
            "job_id": job_id,
        }
