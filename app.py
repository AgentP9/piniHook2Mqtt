"""Flask service translating UniFi Protect webhooks into MQTT events."""

from __future__ import annotations

import atexit
import base64
import json
import logging
import os
import socket
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import paho.mqtt.client as mqtt
from flask import Flask, jsonify, render_template_string, request, send_file

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
    thumbnail_path: str = "/tmp/latest_thumbnail.jpg"

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
            thumbnail_path=os.getenv("THUMBNAIL_PATH", "/tmp/latest_thumbnail.jpg"),
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
        except socket.gaierror as err:
            self._logger.error(
                "MQTT connection failed: cannot resolve host %s:%s (%s)",
                self._config.mqtt_host,
                self._config.mqtt_port,
                err,
            )
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
        if str(reason_code) in {"0", "Success"}:
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


_FRONTEND_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>piniHook2Mqtt \u2013 Latest Event</title>
  <meta http-equiv="refresh" content="5">
  <style>
    * { box-sizing: border-box; }
    body { font-family: sans-serif; margin: 2rem; background: #1a1a2e; color: #eee; }
    h1 { color: #e94560; margin: 0 0 1.5rem; }
    .card { background: #16213e; border-radius: 8px; padding: 1.5rem; max-width: 640px; }
    .card img { width: 100%; border-radius: 4px; display: block; margin-bottom: 1rem; }
    .badge { display: inline-block; padding: .2rem .7rem; border-radius: 4px;
             background: #e94560; font-weight: bold; font-size: .85rem;
             text-transform: uppercase; letter-spacing: .05em; margin-bottom: 1rem; }
    table { width: 100%; border-collapse: collapse; }
    td { padding: .4rem .5rem; border-bottom: 1px solid #0f3460; word-break: break-all; }
    td:first-child { color: #a8a8b3; width: 40%; }
    .muted { color: #a8a8b3; font-style: italic; }
    footer { margin-top: 2rem; color: #a8a8b3; font-size: .8rem; }
  </style>
</head>
<body>
  <h1>Latest Event</h1>
  {% if event %}
  <div class="card">
    <span class="badge">{{ event.type }}</span>
    {% if has_image %}
    <img src="/latest-image?t={{ ts }}" alt="Event thumbnail">
    {% else %}
    <p class="muted">No thumbnail available</p>
    {% endif %}
    <table>
      <tr><td>Camera</td><td>{{ event.camera }}</td></tr>
      {% if event.zone %}
      <tr><td>Zone</td><td>{{ event.zone }}</td></tr>
      {% endif %}
      <tr><td>Event ID</td><td>{{ event.eventId }}</td></tr>
      {% if event.timestamp_display is not none %}
      <tr><td>Timestamp</td><td>{{ event.timestamp_display }}</td></tr>
      {% endif %}
      {% if event.score is not none %}
      <tr><td>Score</td><td>{{ event.score }}</td></tr>
      {% endif %}
    </table>
  </div>
  {% else %}
  <p class="muted">No events received yet.</p>
  {% endif %}
  <footer>Auto-refreshes every 5 seconds.</footer>
</body>
</html>
"""


class LatestEventStore:
    """Thread-safe store for the most recent webhook event metadata and thumbnail."""

    def __init__(self, image_path: str) -> None:
        self._image_path = image_path
        self._lock = threading.Lock()
        self._latest: dict[str, Any] | None = None
        self._has_image = False

    def update(self, meta: dict[str, Any], thumbnail_b64: str | None) -> None:
        """Persist event metadata; write thumbnail JPEG to disk if provided."""
        saved = False
        if thumbnail_b64:
            try:
                raw = thumbnail_b64.split(",", 1)[-1] if "," in thumbnail_b64 else thumbnail_b64
                with open(self._image_path, "wb") as fh:
                    fh.write(base64.b64decode(raw, validate=True))
                saved = True
            except Exception:
                pass
        with self._lock:
            self._latest = dict(meta)
            if thumbnail_b64:
                self._has_image = saved

    def get_latest(self) -> dict[str, Any] | None:
        """Return a copy of the latest event metadata, or None if not yet set."""
        with self._lock:
            return dict(self._latest) if self._latest else None

    @property
    def has_image(self) -> bool:
        """True if a thumbnail has been successfully written to disk."""
        with self._lock:
            return self._has_image

    @property
    def image_path(self) -> str:
        return self._image_path


def _extract_event_meta(
    payload: dict[str, Any], camera_map: dict[str, str]
) -> tuple[dict[str, Any], str | None] | None:
    """Extract event metadata and thumbnail from a raw webhook payload.

    Returns ``(meta, thumbnail_b64)`` for the first valid trigger, or ``None``
    if the payload contains no usable trigger.
    """
    alarm = payload.get("alarm")
    if not isinstance(alarm, dict):
        return None
    triggers = alarm.get("triggers")
    if not isinstance(triggers, list) or not triggers:
        return None
    trigger = next((t for t in triggers if isinstance(t, dict)), None)
    if trigger is None:
        return None
    source_event = trigger.get("sourceEvent") or {}
    if not isinstance(source_event, dict):
        source_event = {}
    camera = str(trigger.get("device", ""))
    meta: dict[str, Any] = {
        "type": str(trigger.get("key", "unknown")),
        "camera": camera,
        "eventId": str(trigger.get("eventId", "")),
        "timestamp": trigger.get("timestamp"),
        "score": source_event.get("score"),
        "zone": camera_map.get(camera),
    }
    thumbnail = alarm.get("thumbnail")
    return meta, thumbnail if isinstance(thumbnail, str) else None


def _format_timestamp(timestamp: Any) -> str | None:
    """Render webhook timestamps in a human-readable UTC format."""
    if timestamp is None:
        return None
    try:
        ts = float(timestamp)
    except (TypeError, ValueError):
        return str(timestamp)
    if abs(ts) > 100_000_000_000:
        ts /= 1000.0
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (OverflowError, OSError, ValueError):
        return str(timestamp)


def create_app(
    config: Config | None = None,
    publisher: MqttPublisher | None = None,
    processor: EventProcessor | None = None,
    event_store: "LatestEventStore | None" = None,
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

    latest_store = event_store or LatestEventStore(runtime_config.thumbnail_path)

    app = Flask(__name__)
    app.config["service_config"] = runtime_config
    app.config["event_processor"] = event_processor
    app.config["mqtt_publisher"] = mqtt_publisher
    app.config["service_logger"] = logger
    app.config["event_store"] = latest_store

    @app.get("/")
    def index() -> tuple[Any, int]:
        event = latest_store.get_latest()
        if event is not None:
            event["timestamp_display"] = _format_timestamp(event.get("timestamp"))
        return (
            render_template_string(
                _FRONTEND_HTML,
                event=event,
                has_image=latest_store.has_image,
                ts=int(time.time()),
            ),
            200,
        )

    @app.get("/latest-image")
    def latest_image() -> tuple[Any, int]:
        if not latest_store.has_image or not os.path.exists(latest_store.image_path):
            return jsonify({"error": "No image available"}), 404
        return send_file(latest_store.image_path, mimetype="image/jpeg"), 200

    @app.get("/health")
    def health() -> tuple[Any, int]:
        return jsonify({"status": "healthy"}), 200

    @app.post("/webhook")
    def webhook() -> tuple[Any, int]:
        logger.info("Received webhook")
        logger.debug("Incoming webhook body: %s", request.get_data(as_text=True))
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"status": "error", "error": "Invalid JSON body"}), 400

        try:
            processed = event_processor.process_webhook(payload)
            result = _extract_event_meta(payload, runtime_config.camera_map)
            if result is not None:
                meta, thumbnail = result
                latest_store.update(meta, thumbnail)
            return jsonify({"status": "ok", "processed": processed}), 200
        except ValueError:
            return jsonify({"status": "error", "error": "Invalid webhook payload"}), 400
        except Exception:
            logger.exception("Unexpected webhook processing error")
            return jsonify({"status": "error", "error": "Internal server error"}), 500

    def shutdown() -> None:
        event_processor.stop()
        mqtt_publisher.stop()

    atexit.register(shutdown)
    return app


def run_server(application: Flask | None = None) -> None:
    """Serve the Flask app using Waitress for production-safe deployment."""
    from waitress import serve

    app_instance = application or create_app()
    cfg: Config = app_instance.config["service_config"]
    serve(app_instance, host=cfg.host, port=cfg.port)


if __name__ == "__main__":
    run_server()
