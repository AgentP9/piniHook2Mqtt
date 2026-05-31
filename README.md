# piniHook2Mqtt

A small tool translating a webhook to MQTT.

## Local webhook listener

This repository includes a tiny Dockerized webhook listener that exposes:

- `POST /webhook`

Incoming requests are dumped to the container logs for inspection.

### Run

```bash
docker compose up --build
```

### Test

```bash
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -d '{"message":"hello"}'
```
