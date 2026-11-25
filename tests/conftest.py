import os
import pytest

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.database.base import Base
from redis import Redis


@pytest.fixture(scope="session")
def test_engine():
    load_dotenv(override=True)

    test_database_url = os.getenv("TEST_DATABASE_URL")

    if not test_database_url:
        raise RuntimeError("TEST_DATABASE_URL is not configured")

    engine = create_engine(test_database_url)
    print("Connected test database:", engine.url.database)

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


@pytest.fixture(scope="session")
def test_redis():
    load_dotenv(override=True)

    redis_url = os.getenv("TEST_REDIS_URL")

    if not redis_url:
        raise RuntimeError("TEST_REDIS_URL is not configured")

    client = Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_timeout=None,
        socket_connect_timeout=5
    )

    client.ping()

    yield client

    client.close()


@pytest.fixture
def clean_test_redis(test_redis):
    # Clean only the dedicated test Redis database.
    test_redis.flushdb()

    yield test_redis

    test_redis.flushdb()
