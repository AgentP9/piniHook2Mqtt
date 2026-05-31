from flask import Flask, jsonify, request

app = Flask(__name__)


@app.post("/webhook")
def webhook():
    payload = request.get_json(silent=True)
    if payload is None:
        payload = request.get_data(as_text=True)

    dump = {
        "method": request.method,
        "path": request.path,
        "headers": dict(request.headers),
        "query": request.args.to_dict(flat=False),
        "payload": payload,
    }
    print(f"Webhook received: {dump}", flush=True)
    return jsonify({"status": "ok"}), 200


@app.get("/health")
def health():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
