import json
import logging
import time
import unittest

from app import Config, EventProcessor, create_app


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def publish(self, topic: str, payload: str, qos: int, retain: bool) -> None:
        self.messages.append(
            {"topic": topic, "payload": payload, "qos": qos, "retain": retain}
        )

    def stop(self) -> None:
        return


class WebhookServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger("test")
        self.publisher = FakePublisher()
        self.config = Config(
            mqtt_topic_events="unifi/protect/event",
            dedup_seconds=30,
            presence_timeout=1,
            camera_map={"8CEDE174492C": "hausdurchgang_nord"},
        )
        self.processor = EventProcessor(self.config, self.publisher, self.logger)
        self.app = create_app(
            config=self.config, publisher=self.publisher, processor=self.processor
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.processor.stop()

    def test_health_endpoint(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(200, response.status_code)
        self.assertEqual({"status": "healthy"}, response.get_json())

    def test_webhook_publishes_normalized_event_and_presence_on(self) -> None:
        payload = {
            "alarm": {
                "triggers": [
                    {
                        "key": "person",
                        "device": "8CEDE174492C",
                        "eventId": "testEventId",
                        "timestamp": 1780215017758,
                    }
                ]
            }
        }

        response = self.client.post("/webhook", json=payload)
        self.assertEqual(200, response.status_code)
        self.assertEqual(2, len(self.publisher.messages))

        event_message = self.publisher.messages[0]
        self.assertEqual("unifi/protect/event", event_message["topic"])
        event_payload = json.loads(str(event_message["payload"]))
        self.assertEqual("unifi_protect", event_payload["source"])
        self.assertEqual("person", event_payload["type"])
        self.assertEqual("8CEDE174492C", event_payload["camera"])
        self.assertEqual("hausdurchgang_nord", event_payload["zone"])
        self.assertEqual("testEventId", event_payload["eventId"])
        self.assertEqual(1780215017758, event_payload["timestamp"])

        presence_message = self.publisher.messages[1]
        self.assertEqual(
            "unifi/protect/presence/hausdurchgang_nord", presence_message["topic"]
        )
        self.assertEqual("ON", presence_message["payload"])

    def test_webhook_deduplicates_camera_and_type(self) -> None:
        payload = {
            "alarm": {
                "triggers": [
                    {
                        "key": "person",
                        "device": "8CEDE174492C",
                        "eventId": "first",
                        "timestamp": 1780215017758,
                    }
                ]
            }
        }

        first_response = self.client.post("/webhook", json=payload)
        second_response = self.client.post("/webhook", json=payload)

        self.assertEqual(200, first_response.status_code)
        self.assertEqual(200, second_response.status_code)
        self.assertEqual(2, len(self.publisher.messages))

    def test_presence_timeout_publishes_off(self) -> None:
        self.processor.start()
        payload = {
            "alarm": {
                "triggers": [
                    {
                        "key": "person",
                        "device": "8CEDE174492C",
                        "eventId": "timeout",
                        "timestamp": 1780215017758,
                    }
                ]
            }
        }
        response = self.client.post("/webhook", json=payload)
        self.assertEqual(200, response.status_code)

        time.sleep(2.1)
        self.assertGreaterEqual(len(self.publisher.messages), 3)
        self.assertEqual("OFF", self.publisher.messages[-1]["payload"])

    def test_webhook_rejects_invalid_json_body(self) -> None:
        response = self.client.post(
            "/webhook", data="not-json", headers={"Content-Type": "application/json"}
        )
        self.assertEqual(400, response.status_code)
        self.assertEqual("error", response.get_json()["status"])


if __name__ == "__main__":
    unittest.main()
