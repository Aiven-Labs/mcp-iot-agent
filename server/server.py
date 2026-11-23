"""Tetris table MCP server.

Exposes tools that publish JSON commands to an Aiven for Kafka topic.
table_lights.py on the Raspberry Pi consumes the topic and drives the LEDs.
"""
import hmac
import json
import os
import time

from confluent_kafka import Producer
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

TOPIC = os.environ.get("KAFKA_TOPIC", "table-lights")
AUTH_TOKEN = os.environ["MCP_AUTH_TOKEN"]

# Effects the table understands. Keep in sync with EFFECTS in bridge/table_lights.py.
EFFECTS = ("Solid", "Rainbow", "Twinkle", "Breathe", "Chase")

# Aiven Kafka client-cert auth. PEM contents come in as env vars, so no
# files need to exist in the container.
producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SSL",
    "ssl.ca.pem": os.environ["KAFKA_CA_CERT"],
    "ssl.certificate.pem": os.environ["KAFKA_ACCESS_CERT"],
    "ssl.key.pem": os.environ["KAFKA_ACCESS_KEY"],
})

# Remembers the last command sent. The server can't see the table, so this is
# the "desired state", not a live reading from the lights.
last_state: dict = {"color": None, "brightness": None, "effect": None}

mcp = FastMCP(
    "tetris-table",
    stateless_http=True,
    host="0.0.0.0",
    # Aiven's proxy sets its own Host header; auth is the bearer token below.
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)


def publish(cmd: str, **fields) -> None:
    """Send a command to Kafka and raise if it wasn't delivered."""
    msg = {"v": 1, "ts": time.time(), "cmd": cmd, **fields}
    errors: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    producer.produce(TOPIC, json.dumps(msg).encode(), callback=on_delivery)
    undelivered = producer.flush(10)
    if errors or undelivered:
        # Surfaces to the agent as a tool error instead of a false "success"
        raise RuntimeError(f"Kafka delivery failed: {errors[0] if errors else 'timed out'}")


@mcp.tool()
def set_color(r: int, g: int, b: int) -> str:
    """Set the Tetris table's lights to a solid RGB colour (each value 0-255). Stops any running effect."""
    rgb = [max(0, min(255, v)) for v in (r, g, b)]
    publish("set_color", rgb=rgb)
    last_state["color"] = rgb
    last_state["effect"] = "Solid"
    return f"Colour set to {rgb}"


@mcp.tool()
def set_brightness(brightness: int) -> str:
    """Set overall brightness, 0 (off) to 255 (full). The table caps it at a safe maximum."""
    value = max(0, min(255, brightness))
    publish("set_brightness", brightness=value)
    last_state["brightness"] = value
    return f"Brightness set to {value}"


@mcp.tool()
def set_effect(effect: str) -> str:
    """Start an animation: Solid, Rainbow, Twinkle (gentle sparkle), Breathe (pulses) or Chase.
    Twinkle, Breathe and Chase use the colour last set with set_color."""
    match = next((e for e in EFFECTS if e.lower() == effect.strip().lower()), None)
    if match is None:
        raise ValueError(f"Unknown effect '{effect}'. Choose one of: {', '.join(EFFECTS)}")
    publish("set_effect", effect=match)
    last_state["effect"] = match
    return f"Effect set to {match}"


@mcp.tool()
def get_state() -> dict:
    """Return the last colour, brightness and effect that were requested."""
    return last_state


class BearerAuth:
    """ASGI middleware: reject requests without the correct bearer token."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope["headers"])
            supplied = headers.get(b"authorization", b"").decode()
            expected = f"Bearer {AUTH_TOKEN}"
            if not hmac.compare_digest(supplied, expected):
                response = JSONResponse({"error": "unauthorized"}, status_code=401)
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)


app = BearerAuth(mcp.streamable_http_app())

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
