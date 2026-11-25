import logging
import threading
from uuid import uuid4

from datetime import datetime
from typing import Any

from app.database.connection import SessionLocal
from app.repositories.outbox_repository import OutboxRepository
from app.queue.redis_stream import RedisStream

logger = logging.getLogger(__name__)


class OutboxDispatcher:

    def __init__(
        self,
        redis_stream: RedisStream,
        poll_interval: float = 1.0,
        batch_size: int = 10,
        claim_duration_seconds: int = 60,
    ) -> None:

        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")

        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        if claim_duration_seconds <= 0:
            raise ValueError("claim_duration_seconds must be positive")

        self.redis_stream = redis_stream
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.claim_duration_seconds = claim_duration_seconds

        self.dispatcher_id = str(uuid4())

        self.shutdown_event = threading.Event()

    def claim_events(self):
        db = SessionLocal()

        try:
            repository = OutboxRepository(db)

            events = repository.claim_events(
                dispatcher_id=self.dispatcher_id,
                batch_size=self.batch_size,
                claim_duration_seconds=self.claim_duration_seconds,
            )

            event_data = [
                {
                    "id": event.id,
                    "event_type": event.event_type,
                    "aggregate_id": event.aggregate_id,
                    "payload": event.payload,
                }
                for event in events
            ]

            # Persist claims before communicating with Redis.
            db.commit()

            return event_data

        except Exception:
            db.rollback()
            logger.exception("Failed to claim outbox events")
            raise

        finally:
            db.close()

    def renew_event_claim(self, event_id: str) -> bool:
        db = SessionLocal()

        try:
            repository = OutboxRepository(db)

            renewed = repository.renew_claim(
                event_id=event_id,
                dispatcher_id=self.dispatcher_id,
                claim_duration_seconds=self.claim_duration_seconds,
            )

            db.commit()
            return renewed

        except Exception:
            db.rollback()
            logger.exception(
                "Failed to renew outbox claim: %s",
                event_id,
            )
            raise

        finally:
            db.close()

    def process_event(self, event: dict[str, Any]) -> None:
        event_id = event["id"]

        try:
            # Step 1: Renew the claim before processing.
            renewed = self.renew_event_claim(event_id)

            if not renewed:
                logger.warning(
                    "Skipping outbox event %s: claim is no longer valid",
                    event_id,
                )
                return

            # Step 2: Extract and validate the event data.
            if event["event_type"] != "job.retry_scheduled":
                raise ValueError(f"Unsupported event type: {event['event_type']}")

            payload = event["payload"]

            job_id = payload["job_id"]
            retry_at = datetime.fromisoformat(payload["retry_at"])

            # Step 3: Publish the retry instruction to Redis.
            self.redis_stream.schedule_delayed_job(
                job_id=job_id,
                retry_at=retry_at,
            )

            # Step 4: Mark the event as published in PostgreSQL.
            db = SessionLocal()

            try:
                repository = OutboxRepository(db)

                published = repository.mark_published(
                    event_id=event_id,
                    dispatcher_id=self.dispatcher_id,
                )

                db.commit()

                if published:
                    logger.info(
                        "Outbox event %s published for job %s",
                        event_id,
                        job_id,
                    )
                else:
                    logger.warning(
                        "Could not mark outbox event %s as published: "
                        "claim is no longer valid",
                        event_id,
                    )

            except Exception:
                db.rollback()
                raise

            finally:
                db.close()

        except Exception as exc:
            logger.exception(
                "Failed to process outbox event %s",
                event_id,
            )

            db = SessionLocal()

            try:
                repository = OutboxRepository(db)

                repository.mark_failed(
                    event_id=event_id,
                    dispatcher_id=self.dispatcher_id,
                    error=str(exc),
                )

                db.commit()

            except Exception:
                db.rollback()
                logger.exception(
                    "Failed to record outbox event failure: %s",
                    event_id,
                )

            finally:
                db.close()

    def dispatch_once(self) -> int:
        """Claim and process one batch of outbox events."""

        events = self.claim_events()

        if not events:
            return 0

        logger.info(
            "Claimed %s outbox events",
            len(events),
        )

        for event in events:
            self.process_event(event)

        return len(events)

    def run(self) -> None:
        """Continuously process outbox events until shutdown."""

        logger.info(
            "Outbox dispatcher started: %s",
            self.dispatcher_id,
        )

        while not self.shutdown_event.is_set():
            try:
                processed = self.dispatch_once()

            except Exception:
                logger.exception("Outbox dispatch cycle failed")
                processed = 0

            # Avoid unnecessary polling when no events are available.
            if processed == 0:
                self.shutdown_event.wait(self.poll_interval)

        logger.info("Outbox dispatcher stopped")

    def stop(self) -> None:
        """Request graceful shutdown."""

        logger.info("Stopping outbox dispatcher...")
        self.shutdown_event.set()
