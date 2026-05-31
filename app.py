"""Flask service translating UniFi Protect webhooks into MQTT events."""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import paho.mqtt.client as mqtt
from flask import Flask, jsonify, request

DEFAULT_SOURCE = "unifi_protect"
DEFAULT_PRESENCE_TOPIC_PREFIX = "unifi/protect/presence"


def parse_bool(value: str | None, default: bool = False) -> bool:
    """Parse common boolean string values from environment variables."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_int(value: str | None, default: int) -> int:
    """Parse integer environment values with a safe default fallback."""
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_camera_map(value: str | None) -> dict[str, str]:
    """Parse CAMERA_MAP values like `A=zone_a,B=zone_b`."""
    mapping: dict[str, str] = {}
    if not value:
        return mapping
    for pair in value.split(","):
        if "=" not in pair:
            continue
        camera, zone = pair.split("=", 1)
        camera_key = camera.strip()
        zone_name = zone.strip()
        if camera_key and zone_name:
            mapping[camera_key] = zone_name
    return mapping


@dataclass(frozen=True)
class Config:
    """Runtime configuration loaded only from environment variables."""

    host: str = "0.0.0.0"
    port: int = 8080
    mqtt_host: str = "mosquitto"
    mqtt_port: int = 1883
    mqtt_user: str = ""
    mqtt_password: str = ""
    mqtt_topic_events: str = "unifi/protect/event"
    mqtt_qos: int = 1
    mqtt_retain: bool = False
    dedup_seconds: int = 30
    presence_timeout: int = 180
    camera_map: dict[str, str] = field(default_factory=dict)
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        """Build a configuration object from environment variables."""
        return cls(
            host=os.getenv("HOST", "0.0.0.0"),
            port=parse_int(os.getenv("PORT"), 8080),
            mqtt_host=os.getenv("MQTT_HOST", "mosquitto"),
            mqtt_port=parse_int(os.getenv("MQTT_PORT"), 1883),
            mqtt_user=os.getenv("MQTT_USER", ""),
            mqtt_password=os.getenv("MQTT_PASSWORD", ""),
            mqtt_topic_events=os.getenv("MQTT_TOPIC_EVENTS", "unifi/protect/event"),
            mqtt_qos=parse_int(os.getenv("MQTT_QOS"), 1),
            mqtt_retain=parse_bool(os.getenv("MQTT_RETAIN"), False),
            dedup_seconds=max(0, parse_int(os.getenv("DEDUP_SECONDS"), 30)),
            presence_timeout=max(1, parse_int(os.getenv("PRESENCE_TIMEOUT"), 180)),
            camera_map=parse_camera_map(os.getenv("CAMERA_MAP")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


class MqttPublisher:
    """Thin MQTT wrapper with reconnect handling and publish helper."""

    def __init__(self, config: Config, logger: logging.Logger) -> None:
        self._config = config
        self._logger = logger
        self._client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        if config.mqtt_user:
            self._client.username_pw_set(config.mqtt_user, config.mqtt_password or None)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)

    def connect(self) -> None:
        """Connect to broker and start network loop in the background."""
        try:
            self._client.connect(self._config.mqtt_host, self._config.mqtt_port)
            self._client.loop_start()
        except Exception:
            self._logger.exception("MQTT connection failed")

    def publish(self, topic: str, payload: str, qos: int, retain: bool) -> None:
        """Publish payloads and log transport-layer publish errors."""
        result = self._client.publish(topic, payload=payload, qos=qos, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            self._logger.warning("MQTT publish failed on topic %s (rc=%s)", topic, result.rc)

    def stop(self) -> None:
        """Stop MQTT loop and disconnect gracefully."""
        self._client.loop_stop()
        self._client.disconnect()

    def _on_connect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _flags: dict[str, Any],
        reason_code: Any,
        _properties: Any = None,
    ) -> None:
        if str(reason_code) == "0":
            self._logger.info("MQTT connected")
        else:
            self._logger.warning("MQTT connect returned code %s", reason_code)

    def _on_disconnect(
        self,
        _client: mqtt.Client,
        _userdata: Any,
        _disconnect_flags: Any,
        reason_code: Any,
        _properties: Any = None,
    ) -> None:
        if str(reason_code) != "0":
            self._logger.warning("MQTT disconnected, reconnecting")


class EventProcessor:
    """Normalize events, deduplicate, and emit presence ON/OFF states."""

    def __init__(self, config: Config, publisher: MqttPublisher, logger: logging.Logger) -> None:
        self._config = config
        self._publisher = publisher
        self._logger = logger
        self._lock = threading.Lock()
        self._recent_events: dict[tuple[str, str], float] = {}
        self._presence_deadlines: dict[str, float] = {}
        self._stop_event = threading.Event()
        self._thread_started = False
        self._presence_thread = threading.Thread(
            target=self._presence_worker, name="presence-timeout-worker", daemon=True
        )

    def start(self) -> None:
        """Start background timeout watcher used for OFF presence events."""
        if not self._thread_started:
            self._presence_thread.start()
            self._thread_started = True

    def stop(self) -> None:
        """Stop background timeout watcher."""
        if self._thread_started:
            self._stop_event.set()
            self._presence_thread.join(timeout=2)

    def process_webhook(self, payload: dict[str, Any]) -> int:
        """Validate, normalize, and publish events for all webhook triggers."""
        triggers = self._extract_triggers(payload)
        processed = 0
        for trigger in triggers:
            event = self._normalize_trigger(trigger)
            if event is None:
                continue
            if self._is_duplicate(event["camera"], event["type"]):
                self._logger.info("Duplicate event ignored")
                continue
            self._publish_event(event)
            processed += 1
        return processed

    def _extract_triggers(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        alarm = payload.get("alarm")
        if not isinstance(alarm, dict):
            raise ValueError("Missing or invalid alarm object")
        triggers = alarm.get("triggers")
        if not isinstance(triggers, list):
            raise ValueError("Missing or invalid alarm.triggers array")
        return [item for item in triggers if isinstance(item, dict)]

    def _normalize_trigger(self, trigger: dict[str, Any]) -> dict[str, Any] | None:
        try:
            event_type = str(trigger["key"])
            camera = str(trigger["device"])
            event_id = str(trigger["eventId"])
            timestamp = int(trigger["timestamp"])
        except (KeyError, ValueError, TypeError):
            self._logger.warning("Invalid trigger payload skipped")
            return None

        event: dict[str, Any] = {
            "source": DEFAULT_SOURCE,
            "type": event_type,
            "camera": camera,
            "eventId": event_id,
            "timestamp": timestamp,
        }
        zone = self._config.camera_map.get(camera)
        if zone:
            event["zone"] = zone
        return event

    def _is_duplicate(self, camera: str, event_type: str) -> bool:
        if self._config.dedup_seconds <= 0:
            return False

        now = time.time()
        key = (camera, event_type)
        with self._lock:
            previous = self._recent_events.get(key)
            self._recent_events[key] = now

        if previous is None:
            return False
        return (now - previous) < self._config.dedup_seconds

    def _publish_event(self, event: dict[str, Any]) -> None:
        self._publisher.publish(
            self._config.mqtt_topic_events,
            json.dumps(event, separators=(",", ":")),
            qos=self._config.mqtt_qos,
            retain=self._config.mqtt_retain,
        )
        self._logger.info(
            "Event %s from %s", event["type"], event.get("zone", event["camera"])
        )
        self._logger.info("Published MQTT event")
        self._publish_presence_on(event)

    def _publish_presence_on(self, event: dict[str, Any]) -> None:
        target = event.get("zone", event["camera"])
        topic = f"{DEFAULT_PRESENCE_TOPIC_PREFIX}/{target}"
        self._publisher.publish(
            topic,
            "ON",
            qos=self._config.mqtt_qos,
            retain=self._config.mqtt_retain,
        )
        with self._lock:
            self._presence_deadlines[target] = time.time() + self._config.presence_timeout

    def _presence_worker(self) -> None:
        while not self._stop_event.wait(timeout=1):
            expired_targets: list[str] = []
            now = time.time()
            with self._lock:
                for target, deadline in list(self._presence_deadlines.items()):
                    if now >= deadline:
                        expired_targets.append(target)
                        del self._presence_deadlines[target]
            for target in expired_targets:
                topic = f"{DEFAULT_PRESENCE_TOPIC_PREFIX}/{target}"
                self._publisher.publish(
                    topic,
                    "OFF",
                    qos=self._config.mqtt_qos,
                    retain=self._config.mqtt_retain,
                )
                self._logger.info("Presence timeout reached")


def create_app(
    config: Config | None = None,
    publisher: MqttPublisher | None = None,
    processor: EventProcessor | None = None,
) -> Flask:
    """Create and configure the Flask application and background services."""
    runtime_config = config or Config.from_env()
    log_level = getattr(logging, runtime_config.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )
    logger = logging.getLogger("piniHook2Mqtt")

    mqtt_publisher = publisher or MqttPublisher(runtime_config, logger)
    if publisher is None:
        mqtt_publisher.connect()

    event_processor = processor or EventProcessor(runtime_config, mqtt_publisher, logger)
    if processor is None:
        event_processor.start()

    app = Flask(__name__)
    app.config["service_config"] = runtime_config
    app.config["event_processor"] = event_processor
    app.config["mqtt_publisher"] = mqtt_publisher
    app.config["service_logger"] = logger

    @app.get("/health")
    def health() -> tuple[Any, int]:
        return jsonify({"status": "healthy"}), 200

    @app.post("/webhook")
    def webhook() -> tuple[Any, int]:
        logger.info("Received webhook")
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"status": "error", "error": "Invalid JSON body"}), 400

        try:
            processed = event_processor.process_webhook(payload)
            return jsonify({"status": "ok", "processed": processed}), 200
        except ValueError as exc:
            return jsonify({"status": "error", "error": str(exc)}), 400
        except Exception:
            logger.exception("Unexpected webhook processing error")
            return jsonify({"status": "error", "error": "Internal server error"}), 500

    def shutdown() -> None:
        event_processor.stop()
        mqtt_publisher.stop()

    atexit.register(shutdown)
    return app


if __name__ == "__main__":
    application = create_app()
    cfg: Config = application.config["service_config"]
    application.run(host=cfg.host, port=cfg.port)
