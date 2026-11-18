"""Bridge: consumes the table-lights Kafka topic and drives WLED over the LAN.

Runs on your home network. It only makes outbound connections (to Aiven
Kafka and to WLED), so no ports need opening.
"""
import json
import os

import requests
from confluent_kafka import Consumer

WLED_URL = f"http://{os.environ['WLED_IP']}/json/state"
TOPIC = os.environ.get("KAFKA_TOPIC", "table-lights")

consumer = Consumer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SSL",
    "ssl.ca.location": os.environ["KAFKA_CA_FILE"],
    "ssl.certificate.location": os.environ["KAFKA_CERT_FILE"],
    "ssl.key.location": os.environ["KAFKA_KEY_FILE"],
    "group.id": "wled-bridge",
    # Don't replay old commands after a restart; only act on new ones.
    "auto.offset.reset": "latest",
})
consumer.subscribe([TOPIC])


def to_wled(msg: dict) -> dict | None:
    match msg["cmd"]:
        case "set_color":
            # fx 0 = Solid, so a colour change cancels any running effect
            return {"on": True, "seg": [{"fx": 0, "col": [msg["rgb"]]}]}
        case "set_brightness":
            return {"on": msg["brightness"] > 0, "bri": msg["brightness"]}
        case "set_effect":
            return {"on": True, "seg": [{"fx": msg["effect"]}]}
    return None


def resolve_effect(state: dict) -> dict:
    """WLED wants an effect index; look names up in its /json/eff list."""
    seg = state.get("seg", [{}])[0]
    if isinstance(seg.get("fx"), str):
        names = requests.get(WLED_URL.replace("/state", "/eff"), timeout=5).json()
        seg["fx"] = names.index(seg["fx"]) if seg["fx"] in names else 0
    return state


print(f"Listening on '{TOPIC}', forwarding to {WLED_URL}")
while True:
    m = consumer.poll(1.0)
    if m is None or m.error():
        continue
    try:
        state = to_wled(json.loads(m.value()))
        if state:
            requests.post(WLED_URL, json=resolve_effect(state), timeout=5).raise_for_status()
    except Exception as e:  # keep the bridge alive on bad messages or a WLED hiccup
        print("skipped message:", e)
