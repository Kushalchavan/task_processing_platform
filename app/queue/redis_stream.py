from typing import cast, Any
from datetime import datetime

from app.database.redis import redis_client

STREAM_NAME = "nexus:jobs"
CONSUMER_GROUP = "nexus-workers"
DELAYED_JOBS_KEY = "nexus:delayed_jobs"


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

    def claim_pending_messages(
        self,
        consumer_name: str,
        min_idle_time: int = 60000,
        start_id: str = "0-0",
        count: int = 10,
    ) -> Any:
        return self.redis.xautoclaim(
            name=STREAM_NAME,
            groupname=CONSUMER_GROUP,
            consumername=consumer_name,
            min_idle_time=min_idle_time,
            start_id=start_id,
            count=count,
        )

    def claim_one_pending(
        self,
        consumer_name: str,
        min_idle_time: int = 60000,
    ) -> dict[str, str] | None:
        _, messages, _ = self.claim_pending_messages(
            consumer_name=consumer_name,
            min_idle_time=min_idle_time,
        )

        if not messages:
            return None

        message_id, data = messages[0]
        job_id = data.get("job_id")

        if not job_id:
            raise ValueError("Redis message is missing job_id")

        return {
            "message_id": message_id,
            "job_id": job_id,
        }

    def schedule_delayed_job(
        self,
        job_id: str,
        retry_at: datetime,
    ) -> int:
        """Store a job in Redis until its scheduled retry time."""

        # Require timezone-aware timestamps to avoid ambiguous scheduling.
        if retry_at.tzinfo is None or retry_at.utcoffset() is None:
            raise ValueError("retry_at must be timezone-aware")

        # Redis Sorted Sets use numerical scores, so convert the
        # scheduled datetime into a Unix timestamp.
        retry_timestamp = retry_at.timestamp()

        return cast(int, self.redis.zadd(DELAYED_JOBS_KEY, {job_id: retry_timestamp}))

    def get_due_delayed_jobs(
        self,
        current_time: datetime,
        count: int = 100,
    ) -> list[str]:
        """Return jobs whose scheduled execution time has arrived."""

        if current_time.tzinfo is None or current_time.utcoffset() is None:
            raise ValueError("current_time must be timezone-aware")

        if count < 1:
            raise ValueError("count must be at least 1")

        current_timestamp = current_time.timestamp()

        # Only retrieve jobs with scores less than or equal to now.
        job_ids = self.redis.zrangebyscore(
            DELAYED_JOBS_KEY,
            min="-inf",
            max=current_timestamp,
            start=0,
            num=count,
        )

        return cast(list[str], job_ids)

    def remove_delayed_job(self, job_id: str) -> int:
        """Remove a job from the delayed-job Sorted Set."""

        return self.redis.zrem(DELAYED_JOBS_KEY, job_id)
