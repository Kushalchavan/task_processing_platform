from datetime import datetime, timedelta, timezone

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.models.outbox_event_model import OutboxEventModel


class OutboxRepository:

    def __init__(self, db: Session):
        self.db = db

    def claim_events(
        self,
        dispatcher_id: str,
        batch_size: int = 10,
        claim_duration_seconds: int = 60,
    ) -> list[OutboxEventModel]:

        if not dispatcher_id.strip():
            raise ValueError("dispatcher_id cannot be empty")

        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        if claim_duration_seconds <= 0:
            raise ValueError("claim_duration_seconds must be positive")

        current_time = datetime.now(timezone.utc)

        claim_expires_at = current_time + timedelta(seconds=claim_duration_seconds)

        events = (
            self.db.query(OutboxEventModel)
            .filter(
                OutboxEventModel.published_at.is_(None),
                or_(
                    OutboxEventModel.claim_expires_at.is_(None),
                    OutboxEventModel.claim_expires_at <= current_time,
                ),
            )
            .order_by(OutboxEventModel.created_at)
            .with_for_update(skip_locked=True)
            .limit(batch_size)
            .all()
        )

        for event in events:
            event.claimed_by = dispatcher_id
            event.claim_expires_at = claim_expires_at

        self.db.flush()

        return events

    def renew_claim(
        self,
        event_id: str,
        dispatcher_id: str,
        claim_duration_seconds: int = 60,
    ) -> bool:

        if claim_duration_seconds <= 0:
            raise ValueError("claim_duration_seconds must be positive")

        current_time = datetime.now(timezone.utc)

        new_expiration = current_time + timedelta(seconds=claim_duration_seconds)

        event = (
            self.db.query(OutboxEventModel)
            .filter(
                OutboxEventModel.id == event_id,
                OutboxEventModel.claimed_by == dispatcher_id,
                OutboxEventModel.published_at.is_(None),
                OutboxEventModel.claim_expires_at > current_time,
            )
            .with_for_update()
            .first()
        )

        if event is None:
            return False

        event.claim_expires_at = new_expiration

        self.db.flush()

        return True

    def mark_published(
        self,
        event_id: str,
        dispatcher_id: str,
    ) -> bool:

        event = (
            self.db.query(OutboxEventModel)
            .filter(
                OutboxEventModel.id == event_id,
                OutboxEventModel.claimed_by == dispatcher_id,
                OutboxEventModel.published_at.is_(None),
                OutboxEventModel.claim_expires_at > datetime.now(timezone.utc),
            )
            .with_for_update()
            .first()
        )

        if event is None:
            return False

        event.published_at = datetime.now(timezone.utc)
        event.claimed_by = None
        event.claim_expires_at = None

        self.db.flush()

        return True

    def mark_failed(
        self,
        event_id: str,
        dispatcher_id: str,
        error: str,
    ) -> bool:

        event = (
            self.db.query(OutboxEventModel)
            .filter(
                OutboxEventModel.id == event_id,
                OutboxEventModel.claimed_by == dispatcher_id,
                OutboxEventModel.published_at.is_(None),
                OutboxEventModel.claim_expires_at > datetime.now(timezone.utc),
            )
            .with_for_update()
            .first()
        )

        if event is None:
            return False

        event.attempts += 1
        event.last_error = error

        self.db.flush()

        return True
