import uuid
import random
from locust import HttpUser, task, between

class DistributedSystemUser(HttpUser):
    wait_time = between(0.1, 1.0)
    
    def on_start(self):
        self.client_id = f"client_{uuid.uuid4().hex[:8]}"
        self.resources = ["resource_A", "resource_B", "resource_C"]
        self.topics = ["topic_X", "topic_Y", "topic_Z"]
        self.cache_keys = ["user_1_profile", "user_2_profile", "config_data"]
        # Use admin token so it can write and lock
        self.client.headers.update({"X-API-Key": "admin-secret-key-123"})

    @task(1)
    def test_distributed_lock(self):
        """Tests acquiring and releasing a lock."""
        resource = random.choice(self.resources)
        
        # Acquire
        acquire_resp = self.client.post("/lock/acquire", json={
            "resource": resource,
            "client_id": self.client_id,
            "type": "exclusive",
            "timeout": 5,
            "ttl": 10
        }, name="/lock/acquire")
        
        if acquire_resp.status_code == 200 and acquire_resp.json().get("status") == "success":
            # Hold lock briefly
            # Release
            self.client.post("/lock/release", json={
                "resource": resource,
                "client_id": self.client_id
            }, name="/lock/release")

    @task(2)
    def test_distributed_queue(self):
        """Tests publishing and polling from the queue."""
        topic = random.choice(self.topics)
        
        # Publish
        self.client.post("/queue/publish", json={
            "topic": topic,
            "payload": {"data": f"Hello from {self.client_id}"}
        }, name="/queue/publish")
        
        # Poll
        poll_resp = self.client.get(f"/queue/poll/{topic}?timeout=1", name="/queue/poll")
        if poll_resp.status_code == 200:
            data = poll_resp.json()
            if data.get("status") == "success":
                msg_id = data["message"]["id"]
                # Ack
                self.client.post("/queue/ack", json={
                    "topic": topic,
                    "message_id": msg_id
                }, name="/queue/ack")

    @task(3)
    def test_cache_coherence(self):
        """Tests cache read and write operations."""
        key = random.choice(self.cache_keys)
        
        # 80% Reads, 20% Writes
        if random.random() < 0.8:
            self.client.get(f"/cache/{key}", name="/cache/read")
        else:
            self.client.post(f"/cache/{key}", json={
                "value": f"updated_by_{self.client_id}"
            }, name="/cache/write")
