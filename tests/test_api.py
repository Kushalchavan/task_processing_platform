import pytest

from job_manager import JobManager
from fastapi.testclient import TestClient

from main import app, get_job_service


@pytest.fixture
def client():
    # setup
    test_manager = JobManager()
    app.dependency_overrides[get_job_service] = lambda: test_manager

    with TestClient(app) as test_client:
        # give client to the test
        yield test_client

    # teardown
    app.dependency_overrides.clear()


def test_create_job(client):
    response = client.post(
        "/jobs", json={"task_type": "send_email", "payload": {"to": "user@example.com"}}
    )

    assert response.status_code == 201
    assert response.json()["task_type"] == "send_email"
    assert response.json()["status"] == "pending"


def test_create_job_with_invalid_data(client):
    response = client.post(
        "/jobs", json={"task_type": "", "payload": {"to": "user@example.com"}}
    )

    assert response.status_code == 422


def test_get_all_jobs(client):
    create_response = client.post(
        "/jobs", json={"task_type": "send_email", "payload": {"to": "user@example.com"}}
    )

    job_id = create_response.json()["id"]
    response = client.get(f"/jobs")

    assert response.status_code == 200
    assert any(job["id"] == job_id for job in response.json())


def test_get_job_id(client):
    create_response = client.post(
        "/jobs", json={"task_type": "send_email", "payload": {"to": "user@example.com"}}
    )

    job_id = create_response.json()["id"]
    response = client.get(f"/jobs/{job_id}")

    assert response.status_code == 200
    assert response.json()["id"] == job_id
