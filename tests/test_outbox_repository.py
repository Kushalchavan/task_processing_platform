from datetime import datetime, timedelta, timezone

from app.models.outbox_event_model import OutboxEventModel
from app.repositories.outbox_repository import OutboxRepository


def test_claim_events_returns_unpublished_events(db_session):

    event = OutboxEventModel(
        event_type="job.retry_scheduled",
        aggregate_id="job-123",
        payload={
            "job_id": "job-123",
            "retry_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        },
    )

    db_session.add(event)
    db_session.flush()

    repository = OutboxRepository(db_session)

    claimed_events = repository.claim_events(
        dispatcher_id="dispatcher-1",
        batch_size=10,
        claim_duration_seconds=60,
    )

    assert len(claimed_events) == 1
    assert claimed_events[0].id == event.id
    assert claimed_events[0].claimed_by == "dispatcher-1"
    assert claimed_events[0].claim_expires_at is not None


def test_claim_events_skips_events_with_active_claim(db_session):

    event = OutboxEventModel(
        event_type="job.retry_scheduled",
        aggregate_id="job-456",
        payload={"job_id": "job-456"},
        claimed_by="dispatcher-1",
        claim_expires_at=(datetime.now(timezone.utc) + timedelta(minutes=5)),
    )

    db_session.add(event)
    db_session.flush()

    repository = OutboxRepository(db_session)

    claimed_events = repository.claim_events(
        dispatcher_id="dispatcher-2",
        batch_size=10,
        claim_duration_seconds=60,
    )

    assert len(claimed_events) == 0
    assert event.claimed_by == "dispatcher-1"


def test_claim_events_reclaims_expired_claim(db_session):

    event = OutboxEventModel(
        event_type="job.retry_scheduled",
        aggregate_id="job-789",
        payload={"job_id": "job-789"},
        claimed_by="dispatcher-1",
        claim_expires_at=(datetime.now(timezone.utc) - timedelta(minutes=5)),
    )

    db_session.add(event)
    db_session.flush()

    repository = OutboxRepository(db_session)

    claimed_events = repository.claim_events(
        dispatcher_id="dispatcher-2",
        batch_size=10,
        claim_duration_seconds=60,
    )

    assert len(claimed_events) == 1
    assert claimed_events[0].id == event.id
    assert claimed_events[0].claimed_by == "dispatcher-2"
    assert claimed_events[0].claim_expires_at > datetime.now(timezone.utc)


def test_mark_published_updates_event(db_session):
    event = OutboxEventModel(
        event_type="job.retry_scheduled",
        aggregate_id="job-101",
        payload={"job_id": "job-101"},
        claimed_by="dispatcher-1",
        claim_expires_at=(datetime.now(timezone.utc) + timedelta(minutes=5)),
    )

    db_session.add(event)
    db_session.flush()

    repository = OutboxRepository(db_session)

    result = repository.mark_published(
        event_id=event.id,
        dispatcher_id="dispatcher-1",
    )

    assert result is True
    assert event.published_at is not None
    assert event.claimed_by is None
    assert event.claim_expires_at is None


def test_mark_failed_records_error_and_preserves_claim(db_session):

    claim_expiration = datetime.now(timezone.utc) + timedelta(minutes=5)

    event = OutboxEventModel(
        event_type="job.retry_scheduled",
        aggregate_id="job-202",
        payload={"job_id": "job-202"},
        claimed_by="dispatcher-1",
        claim_expires_at=claim_expiration,
    )

    db_session.add(event)
    db_session.flush()

    repository = OutboxRepository(db_session)

    result = repository.mark_failed(
        event_id=event.id,
        dispatcher_id="dispatcher-1",
        error="Redis connection failed",
    )

    assert result is True
    assert event.attempts == 1
    assert event.last_error == "Redis connection failed"
    assert event.published_at is None
    assert event.claimed_by == "dispatcher-1"
    assert event.claim_expires_at == claim_expiration


def test_mark_published_rejects_wrong_dispatcher(db_session):

    event = OutboxEventModel(
        event_type="job.retry_scheduled",
        aggregate_id="job-303",
        payload={"job_id": "job-303"},
        claimed_by="dispatcher-1",
        claim_expires_at=(datetime.now(timezone.utc) + timedelta(minutes=5)),
    )

    db_session.add(event)
    db_session.flush()

    repository = OutboxRepository(db_session)

    result = repository.mark_published(
        event_id=event.id,
        dispatcher_id="dispatcher-2",
    )

    assert result is False
    assert event.published_at is None
    assert event.claimed_by == "dispatcher-1"


def test_mark_failed_rejects_wrong_dispatcher(db_session):

    event = OutboxEventModel(
        event_type="job.retry_scheduled",
        aggregate_id="job-404",
        payload={"job_id": "job-404"},
        claimed_by="dispatcher-1",
        claim_expires_at=(datetime.now(timezone.utc) + timedelta(minutes=5)),
    )

    db_session.add(event)
    db_session.flush()

    repository = OutboxRepository(db_session)

    result = repository.mark_failed(
        event_id=event.id,
        dispatcher_id="dispatcher-2",
        error="Redis connection failed",
    )

    assert result is False
    assert event.attempts == 0
    assert event.last_error is None
    assert event.claimed_by == "dispatcher-1"
