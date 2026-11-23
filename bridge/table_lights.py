#!/usr/bin/env python3
"""table_lights.py - runs on the Raspberry Pi in the table.

Consumes the table-lights Kafka topic and drives the LED strip directly with rpi_ws281x,
using the same wiring as tetris_led.py (GPIO 18, 20-LED columns). It only makes outbound
connections to Aiven Kafka, so no ports need opening.

The strip can only have one owner. This shares tetris_led.py's lock file, so the game and this
can never run together: stop this before playing (sudo systemctl stop table-lights).
Needs root for the LED hardware.
"""
import colorsys
import fcntl
import json
import math
import os
import signal
import sys
import time

from confluent_kafka import Consumer
from rpi_ws281x import Color, PixelStrip

LED_COUNT = int(os.environ.get("LED_COUNT", "200"))             # 300 if the side panel is fitted
MAX_BRIGHTNESS = int(os.environ.get("MAX_BRIGHTNESS", "200"))   # power-safety cap, 0-255
TOPIC = os.environ.get("KAFKA_TOPIC", "table-lights")
LOCK_FILE = "/tmp/tetris_led.lock"                              # the game's lock file
COLUMN = 20                                                     # LEDs per column on the table
EFFECTS = ("solid", "rainbow", "twinkle", "breathe", "chase")   # keep in sync with server.py

state = {"rgb": (255, 147, 41), "bri": min(60, MAX_BRIGHTNESS), "last_bri": 60, "effect": "solid"}


def apply(msg):
    """Update the wanted state from one Kafka command."""
    cmd = msg.get("cmd")
    if cmd == "set_color":
        state["rgb"] = tuple(max(0, min(255, int(v))) for v in msg["rgb"])
        state["effect"] = "solid"
    elif cmd == "set_brightness":
        state["bri"] = max(0, min(MAX_BRIGHTNESS, int(msg["brightness"])))
        if state["bri"]:
            state["last_bri"] = state["bri"]
        return
    elif cmd == "set_effect":
        name = str(msg["effect"]).lower()
        if name not in EFFECTS:
            print("unknown effect:", name)
            return
        state["effect"] = name
    else:
        return
    if state["bri"] == 0:                     # a new colour or effect should actually be visible
        state["bri"] = min(state["last_bri"], MAX_BRIGHTNESS)


def pixel(fx, i, t, rgb):
    """Colour of LED number i at time t (seconds) for the given effect."""
    r, g, b = rgb
    if fx == "rainbow":
        hue = ((i // COLUMN) / 10 + t * 0.1) % 1
        return tuple(int(v * 255) for v in colorsys.hsv_to_rgb(hue, 1, 1))
    if fx == "twinkle":
        phase = (math.sin(i * 12.9898) * 43758.5453) % (2 * math.pi)   # a stable random phase per LED
        k = 0.2 + 0.8 * max(0.0, math.sin(t * 1.2 + phase)) ** 6
        return (int(r * k), int(g * k), int(b * k))
    if fx == "breathe":
        k = 0.5 + 0.5 * math.sin(t * 2)
        return (int(r * k), int(g * k), int(b * k))
    if fx == "chase":
        return rgb if (i - int(t * 30)) % 10 < 3 else (0, 0, 0)
    return rgb


def take_lock():
    """Same lock as tetris_led.py: whoever holds it owns the LEDs."""
    f = os.fdopen(os.open(LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o666), "r+")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("The game (or another copy of this) already owns the LEDs. "
                 "Stop it first: sudo pkill -f tetris_led.py   or   sudo systemctl stop table-lights")
    f.seek(0)
    f.truncate()
    f.write(str(os.getpid()))
    f.flush()
    return f


def _stop(*_):
    raise KeyboardInterrupt


def main():
    lock = take_lock()
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SSL",
        "ssl.ca.location": os.environ["KAFKA_CA_FILE"],
        "ssl.certificate.location": os.environ["KAFKA_CERT_FILE"],
        "ssl.key.location": os.environ["KAFKA_KEY_FILE"],
        "group.id": "table-lights-pi",
        "auto.offset.reset": "latest",        # a restart never replays old commands
    })
    consumer.subscribe([TOPIC])
    strip = PixelStrip(LED_COUNT, 18, 800000, 10, False, state["bri"], 0)   # same settings as the game
    strip.begin()
    signal.signal(signal.SIGTERM, _stop)
    print(f"Listening on '{TOPIC}', driving {LED_COUNT} LEDs (brightness cap {MAX_BRIGHTNESS})")
    last = None
    try:
        while True:
            m = consumer.poll(0.03)
            if m is not None and not m.error():
                try:
                    apply(json.loads(m.value()))
                except Exception as e:        # a bad message must not kill the table
                    print("skipped message:", e)
            fx = state["effect"]
            key = (fx, state["rgb"], state["bri"])
            if fx == "solid" and key == last:
                continue                      # nothing changed, nothing to redraw
            last = key
            t = time.monotonic()
            strip.setBrightness(state["bri"])
            for i in range(LED_COUNT):
                strip.setPixelColor(i, Color(*pixel(fx, i, t, state["rgb"])))
            strip.show()
    except KeyboardInterrupt:
        pass
    finally:
        for i in range(LED_COUNT):
            strip.setPixelColor(i, Color(0, 0, 0))
        strip.show()
        consumer.close()
        lock.close()
        print("bye")


if __name__ == "__main__":
    main()
