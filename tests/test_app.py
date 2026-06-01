import json
import logging
import os
import socket
import tempfile
import time
import unittest
from typing import Any
from unittest.mock import Mock, patch

from flask import Flask

from app import (
    Config,
    EventProcessor,
    LatestEventStore,
    MqttPublisher,
    _extract_event_meta,
    create_app,
    run_server,
)


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
        self._tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        self._tmp.close()
        self.event_store = LatestEventStore(self._tmp.name)
        self.app = create_app(
            config=self.config,
            publisher=self.publisher,
            processor=self.processor,
            event_store=self.event_store,
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.processor.stop()
        if os.path.exists(self._tmp.name):
            os.unlink(self._tmp.name)

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
        self.assertEqual(3, len(self.publisher.messages))

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
        self.assertEqual("unifi/protect/hausdurchgang_nord", presence_message["topic"])
        self.assertEqual("ON", presence_message["payload"])

        type_message = self.publisher.messages[2]
        self.assertEqual("unifi/protect/hausdurchgang_nord/person", type_message["topic"])
        self.assertEqual("ON", type_message["payload"])

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
        # First call: event + presence ON + type ON = 3 messages; second call deduped
        self.assertEqual(3, len(self.publisher.messages))

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
        # event + presence ON + type ON + presence OFF + type OFF
        self.assertGreaterEqual(len(self.publisher.messages), 5)
        off_messages = [m for m in self.publisher.messages if m["payload"] == "OFF"]
        self.assertGreaterEqual(len(off_messages), 2)
        topics = {m["topic"] for m in off_messages}
        self.assertIn("unifi/protect/hausdurchgang_nord", topics)
        self.assertIn("unifi/protect/hausdurchgang_nord/person", topics)

    def test_webhook_rejects_invalid_json_body(self) -> None:
        response = self.client.post(
            "/webhook", data="not-json", headers={"Content-Type": "application/json"}
        )
        self.assertEqual(400, response.status_code)
        self.assertEqual("error", response.get_json()["status"])

    def test_webhook_logs_raw_body_at_debug_level(self) -> None:
        raw_body = '{"alarm": {"triggers": []}}'
        with self.assertLogs("piniHook2Mqtt", level="DEBUG") as captured:
            self.client.post("/webhook", data=raw_body, content_type="application/json")
        debug_messages = [m for m in captured.output if "DEBUG" in m]
        self.assertTrue(
            any("Incoming webhook body:" in m and raw_body in m for m in debug_messages),
            f"Expected DEBUG log with raw body not found in: {captured.output}",
        )


class BearerTokenTests(unittest.TestCase):
    """Tests for webhook Bearer-token authentication."""

    def _make_app(self, token: str) -> "Flask":
        logger = logging.getLogger("test")
        publisher = FakePublisher()
        import tempfile, os as _os

        tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        tmp.close()
        self._tmp_path = tmp.name
        config = Config(
            mqtt_topic_events="unifi/protect/event",
            dedup_seconds=0,
            presence_timeout=180,
            webhook_token=token,
        )
        processor = EventProcessor(config, publisher, logger)
        store = LatestEventStore(tmp.name)
        app = create_app(config=config, publisher=publisher, processor=processor, event_store=store)
        self._processor = processor
        return app

    def tearDown(self) -> None:
        self._processor.stop()
        import os as _os

        if _os.path.exists(self._tmp_path):
            _os.unlink(self._tmp_path)

    _valid_payload = {
        "alarm": {
            "triggers": [
                {
                    "key": "person",
                    "device": "CAM1",
                    "eventId": "e1",
                    "timestamp": 1000,
                }
            ]
        }
    }

    def test_webhook_accepted_with_valid_bearer_token(self) -> None:
        client = self._make_app("supersecret").test_client()
        response = client.post(
            "/webhook",
            json=self._valid_payload,
            headers={"Authorization": "Bearer supersecret"},
        )
        self.assertEqual(200, response.status_code)

    def test_webhook_rejected_with_wrong_token(self) -> None:
        client = self._make_app("supersecret").test_client()
        response = client.post(
            "/webhook",
            json=self._valid_payload,
            headers={"Authorization": "Bearer wrongtoken"},
        )
        self.assertEqual(401, response.status_code)
        self.assertEqual("error", response.get_json()["status"])

    def test_webhook_rejected_with_missing_authorization_header(self) -> None:
        client = self._make_app("supersecret").test_client()
        response = client.post("/webhook", json=self._valid_payload)
        self.assertEqual(401, response.status_code)
        self.assertEqual("error", response.get_json()["status"])

    def test_webhook_rejected_with_non_bearer_scheme(self) -> None:
        client = self._make_app("supersecret").test_client()
        response = client.post(
            "/webhook",
            json=self._valid_payload,
            headers={"Authorization": "Basic supersecret"},
        )
        self.assertEqual(401, response.status_code)


class MqttPublisherTests(unittest.TestCase):
    def test_connect_logs_error_without_traceback_for_unresolvable_host(self) -> None:
        logger = Mock()
        publisher = MqttPublisher(Config(mqtt_host="invalid-host"), logger)
        publisher._client = Mock()
        publisher._client.connect.side_effect = socket.gaierror(-2, "Name or service not known")

        publisher.connect()

        logger.error.assert_called_once()
        logger.exception.assert_not_called()
        publisher._client.loop_start.assert_not_called()

    def test_connect_logs_exception_for_unexpected_errors(self) -> None:
        logger = Mock()
        publisher = MqttPublisher(Config(), logger)
        publisher._client = Mock()
        publisher._client.connect.side_effect = RuntimeError("boom")

        publisher.connect()

        logger.exception.assert_called_once_with("MQTT connection failed")
        publisher._client.loop_start.assert_not_called()

    def test_on_connect_treats_success_reason_code_as_connected(self) -> None:
        logger = Mock()
        publisher = MqttPublisher(Config(), logger)

        publisher._on_connect(Mock(), None, {}, "Success")

        logger.info.assert_called_once_with("MQTT connected")
        logger.warning.assert_not_called()

    def test_on_connect_logs_warning_for_non_success_reason_code(self) -> None:
        logger = Mock()
        publisher = MqttPublisher(Config(), logger)

        publisher._on_connect(Mock(), None, {}, "Not authorized")

        logger.warning.assert_called_once_with(
            "MQTT connect returned code %s", "Not authorized"
        )


class ServerStartupTests(unittest.TestCase):
    @patch("waitress.serve")
    def test_run_server_uses_waitress(self, serve_mock: Mock) -> None:
        application = Flask(__name__)
        application.config["service_config"] = Config(port=5050)

        run_server(application=application)

        serve_mock.assert_called_once_with(application, port=5050)


class ConfigTests(unittest.TestCase):
    def test_from_env_requires_webhook_token(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "WEBHOOK_TOKEN environment variable is required"):
                Config.from_env()

    def test_from_env_rejects_blank_webhook_token(self) -> None:
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "   "}, clear=True):
            with self.assertRaisesRegex(ValueError, "WEBHOOK_TOKEN environment variable is required"):
                Config.from_env()

    def test_from_env_strips_webhook_token(self) -> None:
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": " token123 "}, clear=True):
            config = Config.from_env()
        self.assertEqual("token123", config.webhook_token)
        self.assertEqual("unifi/protect/event", config.mqtt_topic_events)
        self.assertEqual("unifi/protect", config.mqtt_topic_root)

    def test_from_env_uses_mqtt_topic_as_root(self) -> None:
        with patch.dict(
            os.environ,
            {"WEBHOOK_TOKEN": "token123", "MQTT_TOPIC": "unifi/surveillance"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual("unifi/surveillance", config.mqtt_topic_root)
        self.assertEqual("unifi/surveillance/event", config.mqtt_topic_events)

    def test_from_env_derives_root_from_mqtt_topic_events(self) -> None:
        with patch.dict(
            os.environ,
            {"WEBHOOK_TOKEN": "token123", "MQTT_TOPIC_EVENTS": "unifi/event"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual("unifi", config.mqtt_topic_root)
        self.assertEqual("unifi/event", config.mqtt_topic_events)

    def test_from_env_uses_default_mqtt_port(self) -> None:
        with patch.dict(os.environ, {"WEBHOOK_TOKEN": "token123"}, clear=True):
            config = Config.from_env()
        self.assertEqual(1883, config.mqtt_port)

    def test_from_env_reads_mqtt_port(self) -> None:
        with patch.dict(
            os.environ,
            {"WEBHOOK_TOKEN": "token123", "MQTT_PORT": "2883"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual(2883, config.mqtt_port)

    def test_from_env_ignores_port_environment_variable(self) -> None:
        with patch.dict(
            os.environ,
            {"WEBHOOK_TOKEN": "token123", "PORT": "9999"},
            clear=True,
        ):
            config = Config.from_env()
        self.assertEqual(8080, config.port)


class LatestEventStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        self._tmp.close()
        self.store = LatestEventStore(self._tmp.name)

    def tearDown(self) -> None:
        if os.path.exists(self._tmp.name):
            os.unlink(self._tmp.name)

    def test_get_latest_returns_none_initially(self) -> None:
        self.assertIsNone(self.store.get_latest())

    def test_has_image_false_initially(self) -> None:
        self.assertFalse(self.store.has_image)

    def test_update_stores_metadata(self) -> None:
        meta = {"type": "person", "camera": "cam1", "eventId": "e1", "timestamp": 1234}
        self.store.update(meta, None)
        self.assertEqual(meta, self.store.get_latest())

    def test_update_without_thumbnail_does_not_set_has_image(self) -> None:
        self.store.update({"type": "person"}, None)
        self.assertFalse(self.store.has_image)

    def test_update_writes_thumbnail_to_disk(self) -> None:
        img_bytes = b"\xff\xd8\xff\xe0fake_jpeg"
        b64 = "data:image/jpeg;base64," + __import__("base64").b64encode(img_bytes).decode()
        self.store.update({"type": "vehicle"}, b64)
        self.assertTrue(self.store.has_image)
        with open(self.store.image_path, "rb") as fh:
            self.assertEqual(img_bytes, fh.read())

    def test_update_overwrites_thumbnail_on_second_call(self) -> None:
        first = b"first_image"
        second = b"second_image"
        b64_first = "data:image/jpeg;base64," + __import__("base64").b64encode(first).decode()
        b64_second = "data:image/jpeg;base64," + __import__("base64").b64encode(second).decode()
        self.store.update({"type": "person"}, b64_first)
        self.store.update({"type": "vehicle"}, b64_second)
        with open(self.store.image_path, "rb") as fh:
            self.assertEqual(second, fh.read())

    def test_update_handles_invalid_base64_gracefully(self) -> None:
        self.store.update({"type": "person"}, "data:image/jpeg;base64,!!!not-valid!!!")
        self.assertFalse(self.store.has_image)

    def test_get_latest_returns_copy(self) -> None:
        meta = {"type": "person"}
        self.store.update(meta, None)
        result = self.store.get_latest()
        assert result is not None
        result["type"] = "mutated"
        self.assertEqual("person", self.store.get_latest()["type"])  # type: ignore[index]


class ExtractEventMetaTests(unittest.TestCase):
    def _trigger(self, **overrides: Any) -> dict[str, Any]:
        base: dict[str, Any] = {
            "key": "person",
            "device": "CAMXYZ",
            "eventId": "eid-1",
            "timestamp": 1000000,
        }
        base.update(overrides)
        return base

    def test_returns_none_for_missing_alarm(self) -> None:
        self.assertIsNone(_extract_event_meta({}, {}))

    def test_returns_none_for_empty_triggers(self) -> None:
        payload = {"alarm": {"triggers": []}}
        self.assertIsNone(_extract_event_meta(payload, {}))

    def test_extracts_basic_metadata(self) -> None:
        payload = {"alarm": {"triggers": [self._trigger()]}}
        result = _extract_event_meta(payload, {})
        assert result is not None
        meta, thumbnail = result
        self.assertEqual("person", meta["type"])
        self.assertEqual("CAMXYZ", meta["camera"])
        self.assertEqual("eid-1", meta["eventId"])
        self.assertEqual(1000000, meta["timestamp"])
        self.assertIsNone(thumbnail)

    def test_extracts_score_from_source_event(self) -> None:
        trigger = self._trigger(sourceEvent={"score": 87})
        payload = {"alarm": {"triggers": [trigger]}}
        result = _extract_event_meta(payload, {})
        assert result is not None
        self.assertEqual(87, result[0]["score"])

    def test_maps_camera_to_zone(self) -> None:
        payload = {"alarm": {"triggers": [self._trigger()]}}
        result = _extract_event_meta(payload, {"CAMXYZ": "front_door"})
        assert result is not None
        self.assertEqual("front_door", result[0]["zone"])

    def test_zone_is_none_when_camera_not_in_map(self) -> None:
        payload = {"alarm": {"triggers": [self._trigger()]}}
        result = _extract_event_meta(payload, {})
        assert result is not None
        self.assertIsNone(result[0]["zone"])

    def test_extracts_thumbnail_from_alarm(self) -> None:
        payload = {
            "alarm": {
                "triggers": [self._trigger()],
                "thumbnail": "data:image/jpeg;base64,abc123",
            }
        }
        result = _extract_event_meta(payload, {})
        assert result is not None
        self.assertEqual("data:image/jpeg;base64,abc123", result[1])

    def test_thumbnail_is_none_when_not_a_string(self) -> None:
        payload = {"alarm": {"triggers": [self._trigger()], "thumbnail": 42}}
        result = _extract_event_meta(payload, {})
        assert result is not None
        self.assertIsNone(result[1])


class WebFrontendTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger("test")
        self.publisher = FakePublisher()
        self.config = Config(
            dedup_seconds=0,
            presence_timeout=180,
            camera_map={"CAM1": "front_door"},
        )
        self.processor = EventProcessor(self.config, self.publisher, self.logger)
        self._tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        self._tmp.close()
        self.event_store = LatestEventStore(self._tmp.name)
        self.app = create_app(
            config=self.config,
            publisher=self.publisher,
            processor=self.processor,
            event_store=self.event_store,
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.processor.stop()
        if os.path.exists(self._tmp.name):
            os.unlink(self._tmp.name)

    def test_index_returns_html_before_any_event(self) -> None:
        response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        self.assertIn(b"Latest Event", response.data)
        self.assertIn(b"No events received yet", response.data)

    def test_index_shows_event_details_after_webhook(self) -> None:
        import base64

        img_bytes = b"\xff\xd8\xff\xe0test"
        b64_thumbnail = "data:image/jpeg;base64," + base64.b64encode(img_bytes).decode()
        payload = {
            "alarm": {
                "triggers": [
                    {
                        "key": "vehicle",
                        "device": "CAM1",
                        "eventId": "evt-42",
                        "timestamp": 9999,
                        "sourceEvent": {"score": 75},
                    }
                ],
                "thumbnail": b64_thumbnail,
            }
        }
        self.client.post("/webhook", json=payload)
        response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        self.assertIn(b"vehicle", response.data)
        self.assertIn(b"CAM1", response.data)
        self.assertIn(b"front_door", response.data)
        self.assertIn(b"evt-42", response.data)
        self.assertIn(b"75", response.data)
        self.assertIn(b"1970-01-01 02:46:39 UTC", response.data)
        self.assertIn(b"/latest-image", response.data)

    def test_latest_image_returns_404_when_no_image(self) -> None:
        self.event_store._has_image = False
        response = self.client.get("/latest-image")
        self.assertEqual(404, response.status_code)

    def test_latest_image_serves_jpeg_after_webhook(self) -> None:
        import base64

        img_bytes = b"\xff\xd8\xff\xe0real_jpeg_payload"
        b64_thumbnail = "data:image/jpeg;base64," + base64.b64encode(img_bytes).decode()
        payload = {
            "alarm": {
                "triggers": [
                    {
                        "key": "person",
                        "device": "CAM1",
                        "eventId": "e1",
                        "timestamp": 1234,
                    }
                ],
                "thumbnail": b64_thumbnail,
            }
        }
        self.client.post("/webhook", json=payload)
        response = self.client.get("/latest-image")
        self.assertEqual(200, response.status_code)
        self.assertEqual("image/jpeg", response.content_type)
        self.assertEqual(img_bytes, response.data)

    def test_latest_event_returns_null_event_before_any_webhook(self) -> None:
        response = self.client.get("/latest-event")
        self.assertEqual(200, response.status_code)
        data = response.get_json()
        self.assertIsNone(data["event"])
        self.assertFalse(data["has_image"])
        self.assertIn("image_ts", data)

    def test_latest_event_returns_event_data_after_webhook(self) -> None:
        payload = {
            "alarm": {
                "triggers": [
                    {
                        "key": "vehicle",
                        "device": "CAM1",
                        "eventId": "evt-json",
                        "timestamp": 9999,
                        "sourceEvent": {"score": 50},
                    }
                ]
            }
        }
        self.client.post("/webhook", json=payload)
        response = self.client.get("/latest-event")
        self.assertEqual(200, response.status_code)
        data = response.get_json()
        event = data["event"]
        self.assertIsNotNone(event)
        self.assertEqual("vehicle", event["type"])
        self.assertEqual("CAM1", event["camera"])
        self.assertEqual("front_door", event["zone"])
        self.assertEqual("evt-json", event["eventId"])
        self.assertEqual(9999, event["timestamp"])
        self.assertEqual(50, event["score"])

    def test_webhook_updates_event_store(self) -> None:
        payload = {
            "alarm": {
                "triggers": [
                    {
                        "key": "person",
                        "device": "CAM1",
                        "eventId": "store-test",
                        "timestamp": 5555,
                    }
                ]
            }
        }
        self.client.post("/webhook", json=payload)
        meta = self.event_store.get_latest()
        self.assertIsNotNone(meta)
        assert meta is not None
        self.assertEqual("person", meta["type"])
        self.assertEqual("CAM1", meta["camera"])
        self.assertEqual("store-test", meta["eventId"])
        self.assertEqual("front_door", meta["zone"])


if __name__ == "__main__":
    unittest.main()
