import uuid

from locust import HttpUser, between, task


class NexusUser(HttpUser):
    # Wait between 1 and 3 seconds between requests.
    wait_time = between(1, 3)

    @task
    def create_job(self):
        payload = {
            "task_type": "print_message",
            "payload": {"message": f"Load test job {uuid.uuid4()}"},
        }

        with self.client.post(
            "/jobs",
            json=payload,
            name="POST /jobs",
            catch_response=True,
        ) as response:

            if response.status_code != 201:
                response.failure(f"Expected 201, received {response.status_code}")
                return

            try:
                data = response.json()

                if not data.get("id"):
                    response.failure("Response does not contain job ID")
                    return

                if data.get("task_type") != "print_message":
                    response.failure("Unexpected task_type")
                    return

                response.success()

            except ValueError:
                response.failure("Response was not valid JSON")
