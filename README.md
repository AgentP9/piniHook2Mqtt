# piniHook2Mqtt

Flask service (served by Waitress in production mode) that translates UniFi Protect webhook payloads into MQTT events.

## Container overview

The container runs `app.py` and exposes:

- `GET /health`
- `POST /webhook`

The image listens on port `8080` inside the container. The provided `docker-compose.yml` publishes it as `4040:8080`.

## Prerequisites

- Docker
- Docker Compose
- A reachable MQTT broker

> [!IMPORTANT]
> The included `docker-compose.yml` only starts this service. It does **not** start an MQTT broker for you. Set `MQTT_HOST` to a broker hostname or IP that the container can reach.

## Start with Docker Compose

Build and start the container:

```bash
MQTT_HOST=<your-mqtt-host> docker compose up --build -d
```

The compose service forwards all documented environment variables from your shell (or `.env` file) into the container.

View logs:

```bash
docker compose logs -f webhook-listener
```

Stop the container:

```bash
docker compose down
```

## Start with `docker run`

```bash
docker build -t pinihook2mqtt .

docker run --rm \
  --name pinihook2mqtt \
  -p 4040:8080 \
  -e MQTT_HOST=<your-mqtt-host> \
  -e MQTT_PORT=1883 \
  pinihook2mqtt
```

## Environment variables

All runtime configuration is done with environment variables.

| Variable | Default | Description |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | HTTP bind address inside the container. |
| `PORT` | `8080` | HTTP port inside the container. |
| `MQTT_HOST` | `mosquitto` | MQTT broker hostname or IP reachable from the container. |
| `MQTT_PORT` | `1883` | MQTT broker port. |
| `MQTT_USER` | empty | Optional MQTT username. |
| `MQTT_PASSWORD` | empty | Optional MQTT password. |
| `MQTT_TOPIC` | `unifi/protect` | Root topic used for all publishes (`<root>/event`, `<root>/<zone-or-camera>`, `<root>/<zone-or-camera>/<type>`). |
| `MQTT_TOPIC_EVENTS` | derived from `MQTT_TOPIC` | Optional explicit event topic. If `MQTT_TOPIC` is unset, this value is used and its parent path becomes the root for presence topics. |
| `MQTT_QOS` | `1` | QoS used for MQTT publishes. |
| `MQTT_RETAIN` | `false` | Whether MQTT publishes are retained. |
| `DEDUP_SECONDS` | `30` | Duplicate suppression window per camera and event type. |
| `PRESENCE_TIMEOUT` | `180` | Seconds before the presence topic for a camera/zone is set to `OFF`. |
| `CAMERA_MAP` | empty | Optional camera-to-zone mapping like `CAMERA1=driveway,CAMERA2=frontdoor`. |
| `LOG_LEVEL` | `INFO` | Application log level. Accepted values: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. At `DEBUG` level the raw incoming webhook body is printed to the console. |
| `WEBHOOK_TOKEN` | required | Required shared secret for webhook security. Every `POST /webhook` request must include an `Authorization: Bearer <token>` header. Requests without a valid token receive `401 Unauthorized`. |

Example with additional settings:

```bash
MQTT_HOST=192.168.1.10 \
MQTT_USER=my-user \
MQTT_PASSWORD=my-password \
CAMERA_MAP=8CEDE174492C=hausdurchgang_nord \
docker compose up --build -d
```

## Test the container

Health check:

```bash
curl http://localhost:4040/health
```

Set `WEBHOOK_TOKEN=mysecret` and send:

```bash
curl -X POST http://localhost:4040/webhook \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer mysecret" \
  -d '{
    "alarm": {
      "triggers": [
        {
          "key": "person",
          "device": "8CEDE174492C",
          "eventId": "testEventId",
          "timestamp": 1780215017758
        }
      ]
    }
  }'
```

## MQTT output

For each accepted trigger, the service publishes:

1. A normalized JSON event to `MQTT_TOPIC_EVENTS`
2. A presence state of `ON` to `<root-topic>/<zone-or-camera>`
3. A presence state of `ON` to `<root-topic>/<zone-or-camera>/<type>` (e.g. `person`, `vehicle`, `animal`)
4. A presence state of `OFF` to both topics after `PRESENCE_TIMEOUT`

`<root-topic>` is `MQTT_TOPIC` when set. Otherwise it is inferred from `MQTT_TOPIC_EVENTS` (for example `MQTT_TOPIC_EVENTS=unifi/event` yields root `unifi`).

If `CAMERA_MAP` contains the camera ID, the mapped zone name is used in the topic path and added as `zone` in the JSON payload.
