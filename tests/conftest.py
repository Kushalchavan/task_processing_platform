import os
import pytest

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from app.database.base import Base
from app.models.job_model import JobModel


@pytest.fixture(scope="session")
def test_engine():
    load_dotenv(override=True)

    test_database_url = os.getenv("TEST_DATABASE_URL")

    if not test_database_url:
        raise RuntimeError("TEST_DATABASE_URL is not configured")

    engine = create_engine(test_database_url)

    Base.metadata.create_all(bind=engine)

    yield engine

    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture
def db_session(test_engine):
    connection = test_engine.connect()
    transaction = connection.begin()

    session = Session(
        bind=connection,
        join_transaction_mode="create_savepoint",
    )

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()
