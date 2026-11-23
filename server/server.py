"""Tetris table MCP server.

Exposes tools that publish JSON commands to an Aiven for Kafka topic.
table_lights.py on the Raspberry Pi consumes the topic and drives the LEDs.
"""
import hmac
import json
import os
import time
from typing import Optional

from confluent_kafka import Producer
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

TOPIC = os.environ.get("KAFKA_TOPIC", "table-lights")
AUTH_TOKEN = os.environ["MCP_AUTH_TOKEN"]

# Effects the table understands. Keep in sync with EFFECTS in bridge/table_lights.py.
EFFECTS = ("Solid", "Rainbow", "Twinkle", "Breathe", "Chase",
           "Fire", "Plasma", "Aurora", "Rain", "Comet", "Fireworks", "Wave")

# Characters the table's font can draw (text is upper-cased). Keep in sync with GLYPHS in table_lights.py.
# '*' draws a heart.
SUPPORTED_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 .!?-:',/+=()<>_*")

COLS, ROWS = 10, 20
# One-letter colour codes for draw/animate. '.' and ' ' are off. Any letter can be redefined via `palette`.
DEFAULT_PALETTE = {
    "r": [255, 0, 0], "g": [0, 255, 0], "b": [0, 0, 255], "y": [255, 200, 0], "o": [255, 100, 0],
    "p": [160, 0, 255], "m": [255, 0, 160], "c": [0, 220, 255], "w": [255, 255, 255], "n": [255, 147, 41],
    "k": [0, 0, 0], ".": [0, 0, 0], " ": [0, 0, 0],
}

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
last_state: dict = {"color": None, "brightness": None, "effect": None, "display": None}

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


def _rgb(name: str, value) -> list[int]:
    if not (isinstance(value, (list, tuple)) and len(value) == 3):
        raise ValueError(f"{name} must be [r, g, b] with values 0-255")
    return [max(0, min(255, int(v))) for v in value]


def _resolve_frames(frames: list[list[str]], palette: Optional[dict]) -> dict:
    """Validate pictures and return only the palette entries they use."""
    colours = {k: v for k, v in DEFAULT_PALETTE.items()}
    for ch, rgb in (palette or {}).items():
        if len(ch) != 1:
            raise ValueError(f"palette keys must be single characters, got {ch!r}")
        colours[ch] = _rgb(f"palette[{ch!r}]", rgb)
    used: dict[str, list[int]] = {}
    for n, rows in enumerate(frames, 1):
        if len(rows) != ROWS:
            raise ValueError(f"frame {n} has {len(rows)} rows, needs exactly {ROWS} (top to bottom)")
        for r, row in enumerate(rows):
            if len(row) != COLS:
                raise ValueError(f"frame {n}, row {r + 1} has {len(row)} characters, needs exactly {COLS}")
            for ch in row:
                if ch not in colours:
                    raise ValueError(f"unknown colour {ch!r} in frame {n}, row {r + 1}: "
                                     f"define it in palette or use one of {''.join(sorted(DEFAULT_PALETTE))}")
                used[ch] = colours[ch]
    return used


@mcp.tool()
def set_color(r: int, g: int, b: int) -> str:
    """Set the Tetris table's lights to a solid RGB colour (each value 0-255). Stops any running effect.
    The colour is also used by effects like Twinkle, Breathe, Chase, Rain, Comet and Wave, and as the
    default text colour."""
    rgb = [max(0, min(255, v)) for v in (r, g, b)]
    publish("set_color", rgb=rgb)
    last_state["color"] = rgb
    last_state["effect"] = "Solid"
    last_state["display"] = None
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
    """Start a built-in animation that runs until something else is set. Choices: Solid, Rainbow
    (colours scroll across), Twinkle (gentle sparkle), Breathe (pulses), Chase (dashes run along the
    strip), Fire (flames rising from the floor), Plasma (swirling colour), Aurora (green-blue curtains),
    Rain (drops fall in the current colour), Comet (two comets run the strip), Fireworks (random bursts),
    Wave (brightness ripples). Twinkle, Breathe, Chase, Rain, Comet and Wave use the colour last set with
    set_color."""
    match = next((e for e in EFFECTS if e.lower() == effect.strip().lower()), None)
    if match is None:
        raise ValueError(f"Unknown effect '{effect}'. Choose one of: {', '.join(EFFECTS)}")
    publish("set_effect", effect=match)
    last_state["effect"] = match
    last_state["display"] = None
    return f"Effect set to {match}"


@mcp.tool()
def show_text(
    text: str,
    color: Optional[list[int]] = None,
    background: Optional[list[int]] = None,
    size: str = "large",
    speed: float = 10.0,
    direction: str = "down",
    scroll: bool = True,
    loop: bool = True,
) -> str:
    """Show text on the table. The table is 10 LEDs wide and 20 tall, so text is rotated to run along the
    long side: tilt your head right to read it with direction "down" (letters' tops point to the right-hand
    column), or left with "up". If it reads upside down for the person looking, switch direction.
    text: letters A-Z (case ignored), digits and . ! ? - : ' , / + = ( ) < > _ and * (a heart); max 200 chars.
    color: [r, g, b], default is the last colour set. background: optional [r, g, b] fill behind the text.
    size: "large" (letters fill the 10-LED width, ~2 letters visible) or "small" (half the width).
    speed: scroll speed in LEDs per second (1-60). scroll: False holds short text still (only about
    2 large or 5 small letters fit; longer text scrolls anyway). loop: True repeats until something else is
    set; False scrolls once and then returns to a solid colour."""
    clean = text.upper()[:200]
    bad = sorted({c for c in clean if c not in SUPPORTED_CHARS})
    if bad:
        raise ValueError(f"Unsupported characters: {' '.join(bad)}. Supported: letters, digits and . ! ? - : ' , / + = ( ) < > _ *")
    if not clean.strip():
        raise ValueError("text is empty")
    if size not in ("large", "small"):
        raise ValueError('size must be "large" or "small"')
    if direction not in ("down", "up"):
        raise ValueError('direction must be "down" or "up"')
    fields: dict = dict(text=clean, size=size, speed=max(1.0, min(60.0, float(speed))),
                        direction=direction, scroll=scroll, loop=loop)
    if color is not None:
        fields["rgb"] = _rgb("color", color)
    if background is not None:
        fields["bg"] = _rgb("background", background)
    publish("show_text", **fields)
    last_state["display"] = f"text: {clean}"
    return f"Showing text {clean!r} ({'scrolling' if scroll else 'static'}, {size}, direction {direction})"


@mcp.tool()
def draw(rows: list[str], palette: Optional[dict[str, list[int]]] = None) -> str:
    """Draw a still picture on the table, pixel by pixel. rows is exactly 20 strings (top to bottom), each
    exactly 10 characters (left to right); each character is one LED. Colour codes: r red, g green,
    b blue, y yellow, o orange, p purple, m magenta, c cyan, w white, n warm white, and '.' (or space or k)
    for off. Add or override codes with palette, e.g. {"s": [255, 20, 147]}. Example of a heart-ish shape
    in the middle rows: "..rr..rr..", ".rrrrrrrr.", ".rrrrrrrr.", "..rrrrrr..", "...rrrr...", "....rr....".
    Use this for shapes, patterns, flags, emoji, borders or anything the built-in effects can't do.
    Pictures keep the current brightness."""
    used = _resolve_frames([rows], palette)
    publish("animate", frames=[rows], palette=used, fps=1, loop=False)
    last_state["display"] = "drawing"
    return "Drawing shown"


@mcp.tool()
def animate(frames: list[list[str]], palette: Optional[dict[str, list[int]]] = None,
            fps: float = 4.0, loop: bool = True) -> str:
    """Play a hand-made animation: a list of frames, each frame is exactly 20 strings (top to bottom) of
    exactly 10 characters (left to right), using the same colour codes and palette as draw. Up to 40
    frames, fps 0.2-30 (frames per second). loop True repeats until something else is set; False plays
    once and holds the last frame. Good for bouncing balls, falling blocks, pulsing shapes, spinners,
    scrolling pictures. Keep animations to a few frames of simple shapes for best results."""
    if not frames:
        raise ValueError("frames is empty")
    if len(frames) > 40:
        raise ValueError("at most 40 frames")
    used = _resolve_frames(frames, palette)
    publish("animate", frames=frames, palette=used, fps=max(0.2, min(30.0, float(fps))), loop=loop)
    last_state["display"] = f"animation, {len(frames)} frames"
    return f"Playing {len(frames)} frames at {fps} fps ({'looping' if loop else 'once'})"


@mcp.tool()
def get_state() -> dict:
    """Return the last colour, brightness, effect and text/picture that were requested. The server cannot
    see the table, so this is what was asked for, not a live reading."""
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
