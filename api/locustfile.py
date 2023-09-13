from locust import HttpUser, between, task


class WebsiteUser(HttpUser):
    @task
    def get_data(self):
        self.client.get("/collections/observations/locations/06260")
