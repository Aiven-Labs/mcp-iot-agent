#!/usr/bin/env python3
"""table_lights.py - runs on the Raspberry Pi in the table.

Consumes the table-lights Kafka topic and drives the 10 x 20 LED matrix directly with rpi_ws281x,
using the same wiring as tetris_led.py (GPIO 18, ten 20-LED columns, snaking). It only makes
outbound connections to Aiven Kafka, so no ports need opening.

The strip can only have one owner. This shares tetris_led.py's lock file, so the game and this
can never run together: stop this before playing (sudo systemctl stop table-lights).
Needs root for the LED hardware.

Commands (JSON, one per Kafka message):
  set_color {rgb}  set_brightness {brightness}  set_effect {effect}
  show_text {text, rgb, bg, size, speed, direction, scroll, loop}
  animate   {frames: [[20 rows of 10 chars], ...], palette: {char: rgb}, fps, loop}
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

COLS, ROWS = 10, 20
LED_COUNT = int(os.environ.get("LED_COUNT", "200"))             # LEDs past the first 200 (side panel) stay dark
MAX_BRIGHTNESS = int(os.environ.get("MAX_BRIGHTNESS", "200"))   # power-safety cap, 0-255
TOPIC = os.environ.get("KAFKA_TOPIC", "table-lights")
LOCK_FILE = "/tmp/tetris_led.lock"                              # the game's lock file
FIRST_COL_DOWN = True                                           # same wiring map as tetris_led.py
EFFECTS = ("solid", "rainbow", "twinkle", "breathe", "chase",   # keep in sync with server.py
           "fire", "plasma", "aurora", "rain", "comet", "fireworks", "wave")
MAX_TEXT, MAX_FRAMES = 200, 40


def _flag(name):
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


# Picture orientation. Row 0 is the "top" (where game pieces spawn), row 19 the floor.
# Either set FLIP_X / FLIP_Y by hand, or point TETRIS_ORIENT_FILE at the game's
# tetris_orientation.json so pictures match the way you set the game up.
FLIP_X, FLIP_Y = _flag("FLIP_X"), _flag("FLIP_Y")
_orient = os.environ.get("TETRIS_ORIENT_FILE")
if _orient:
    try:
        with open(_orient) as f:
            _o = json.load(f)
        if _o.get("fall_along", "long") == "long":
            FLIP_Y = bool(_o.get("flip_fall", FLIP_Y))
            FLIP_X = bool(_o.get("mirror_across", FLIP_X))
    except Exception as e:
        print("ignoring orientation file:", e)

# ----------------------------------------------------------------- font (the game's 3x5 font)
GLYPHS = {
    "0": ["###", "#.#", "#.#", "#.#", "###"], "1": [".#.", "##.", ".#.", ".#.", "###"],
    "2": ["###", "..#", "###", "#..", "###"], "3": ["###", "..#", "###", "..#", "###"],
    "4": ["#.#", "#.#", "###", "..#", "..#"], "5": ["###", "#..", "###", "..#", "###"],
    "6": ["###", "#..", "###", "#.#", "###"], "7": ["###", "..#", "..#", "..#", "..#"],
    "8": ["###", "#.#", "###", "#.#", "###"], "9": ["###", "#.#", "###", "..#", "###"],
    "A": [".#.", "#.#", "###", "#.#", "#.#"], "B": ["##.", "#.#", "##.", "#.#", "##."],
    "C": [".##", "#..", "#..", "#..", ".##"], "D": ["##.", "#.#", "#.#", "#.#", "##."],
    "E": ["###", "#..", "##.", "#..", "###"], "F": ["###", "#..", "##.", "#..", "#.."],
    "G": [".##", "#..", "#.#", "#.#", ".##"], "H": ["#.#", "#.#", "###", "#.#", "#.#"],
    "I": ["###", ".#.", ".#.", ".#.", "###"], "J": ["..#", "..#", "..#", "#.#", ".#."],
    "K": ["#.#", "#.#", "##.", "#.#", "#.#"], "L": ["#..", "#..", "#..", "#..", "###"],
    "M": ["#...#", "##.##", "#.#.#", "#...#", "#...#"], "N": ["#..#", "##.#", "#.##", "#..#", "#..#"],
    "O": [".#.", "#.#", "#.#", "#.#", ".#."], "P": ["##.", "#.#", "##.", "#..", "#.."],
    "Q": [".#.", "#.#", "#.#", "##.", ".##"], "R": ["##.", "#.#", "##.", "#.#", "#.#"],
    "S": [".##", "#..", ".#.", "..#", "##."], "T": ["###", ".#.", ".#.", ".#.", ".#."],
    "U": ["#.#", "#.#", "#.#", "#.#", "###"], "V": ["#.#", "#.#", "#.#", "#.#", ".#."],
    "W": ["#...#", "#...#", "#.#.#", "#.#.#", ".#.#."], "X": ["#.#", "#.#", ".#.", "#.#", "#.#"],
    "Y": ["#.#", "#.#", ".#.", ".#.", ".#."], "Z": ["###", "..#", ".#.", "#..", "###"],
    " ": ["..", "..", "..", "..", ".."], ".": [".", ".", ".", ".", "#"],
    "!": ["#", "#", "#", ".", "#"], "?": ["##.", "..#", ".#.", "...", ".#."],
    "-": ["...", "...", "###", "...", "..."], ":": [".", "#", ".", "#", "."],
    "'": ["#", "#", ".", ".", "."],
    # additions beyond the game's font (keep SUPPORTED_CHARS in server.py in sync)
    ",": [".", ".", ".", "#", "#"], "/": ["..#", "..#", ".#.", "#..", "#.."],
    "+": ["...", ".#.", "###", ".#.", "..."], "=": ["...", "###", "...", "###", "..."],
    "(": [".#", "#.", "#.", "#.", ".#"], ")": ["#.", ".#", ".#", ".#", "#."],
    "<": ["..#", ".#.", "#..", ".#.", "..#"], ">": ["#..", ".#.", "..#", ".#.", "#.."],
    "_": ["...", "...", "...", "...", "###"], "*": ["#.#", "###", "###", ".#.", "..."],  # * = heart
}
FONT = {ch: [tuple(1 if rows[r][c] == "#" else 0 for r in range(5)) for c in range(len(rows[0]))]
        for ch, rows in GLYPHS.items()}


def text_columns(text):
    """Text -> list of 5-high columns, one blank column between letters."""
    cols = []
    for i, ch in enumerate(text):
        if i:
            cols.append((0, 0, 0, 0, 0))
        cols.extend(FONT.get(ch, FONT[" "]))
    return cols


# ----------------------------------------------------------------- state
BLACK = (0, 0, 0)
state = {"rgb": (255, 147, 41), "bri": min(60, MAX_BRIGHTNESS), "last_bri": 60,
         "mode": "effect", "effect": "solid", "t0": 0.0, "text": None, "anim": None, "dirty": True}


def _rgb(v, default=None):
    try:
        return tuple(max(0, min(255, int(x))) for x in v)[:3] if v is not None else default
    except (TypeError, ValueError):
        return default


def xy_to_index(col, row):
    """Matrix (col, row) -> LED index in the chain (column-major snake), as in tetris_led.py."""
    goes_down = FIRST_COL_DOWN if col % 2 == 0 else (not FIRST_COL_DOWN)
    return col * ROWS + (row if goes_down else ROWS - 1 - row)


def put(buf, col, row, rgb):
    """Set one pixel in picture coordinates (flips applied here, once)."""
    if not (0 <= col < COLS and 0 <= row < ROWS):
        return
    if FLIP_X:
        col = COLS - 1 - col
    if FLIP_Y:
        row = ROWS - 1 - row
    i = xy_to_index(col, row)
    if i < len(buf):
        buf[i] = rgb


def build_text(msg):
    text = str(msg["text"]).upper()[:MAX_TEXT]
    cols = text_columns(text)
    large = msg.get("size", "large") != "small"
    if large:      # double every pixel: 6x10 letters that fill the 10-wide table
        cols = [tuple(v for v in c for _ in (0, 1)) for c in cols for _ in (0, 1)]
    gh = 10 if large else 5
    scroll = bool(msg.get("scroll", True)) or len(cols) > ROWS
    return {"lit": [(p, gy) for p, c in enumerate(cols) for gy, v in enumerate(c) if v],
            "width": len(cols), "off": (COLS - gh) // 2, "scroll": scroll,
            "speed": max(1.0, min(60.0, float(msg.get("speed", 10)))),
            "down": msg.get("direction", "down") != "up", "loop": bool(msg.get("loop", True)),
            "rgb": _rgb(msg.get("rgb")), "bg": _rgb(msg.get("bg")), "t0": time.monotonic()}


def build_anim(msg):
    palette = {str(k)[:1]: _rgb(v, BLACK) for k, v in (msg.get("palette") or {}).items()}
    frames = []
    for rows in list(msg["frames"])[:MAX_FRAMES]:
        lit = []
        for r, line in enumerate(list(rows)[:ROWS]):
            for c, ch in enumerate(str(line)[:COLS]):
                rgb = palette.get(ch)
                if rgb and rgb != BLACK:
                    lit.append((c, r, rgb))
        frames.append(lit)
    if not frames:
        raise ValueError("no frames")
    return {"frames": frames, "fps": max(0.2, min(30.0, float(msg.get("fps", 4)))),
            "loop": bool(msg.get("loop", True)), "t0": time.monotonic()}


def apply(msg):
    """Update the wanted state from one Kafka command."""
    cmd = msg.get("cmd")
    if cmd == "set_brightness":
        state["bri"] = max(0, min(MAX_BRIGHTNESS, int(msg["brightness"])))
        if state["bri"]:
            state["last_bri"] = state["bri"]
        state["dirty"] = True
        return
    if cmd == "set_color":
        state["rgb"] = _rgb(msg["rgb"])
        state.update(mode="effect", effect="solid")
    elif cmd == "set_effect":
        name = str(msg["effect"]).lower()
        if name not in EFFECTS:
            print("unknown effect:", name)
            return
        state.update(mode="effect", effect=name, t0=time.monotonic())
    elif cmd == "show_text":
        state.update(mode="text", text=build_text(msg))
    elif cmd == "animate":
        state.update(mode="frames", anim=build_anim(msg))
    else:
        return
    state["dirty"] = True
    if state["bri"] == 0:                     # a new picture should actually be visible
        state["bri"] = min(state["last_bri"], MAX_BRIGHTNESS)


# ----------------------------------------------------------------- effects
def hsv(h, s, v):
    return tuple(int(x * 255) for x in colorsys.hsv_to_rgb(h % 1.0, s, max(0.0, min(1.0, v))))


def sc(rgb, k):
    k = max(0.0, min(1.0, k))
    return (int(rgb[0] * k), int(rgb[1] * k), int(rgb[2] * k))


def rnd(n):
    """Stable pseudo-random 0..1 from a number."""
    x = math.sin(n * 12.9898) * 43758.5453
    return x - math.floor(x)


def fx_rainbow(c, r, t, rgb):
    return hsv(c / COLS + t * 0.1, 1, 1)


def fx_twinkle(c, r, t, rgb):
    phase = rnd(xy_to_index(c, r)) * 2 * math.pi
    return sc(rgb, 0.2 + 0.8 * max(0.0, math.sin(t * 1.2 + phase)) ** 6)


def fx_breathe(c, r, t, rgb):
    return sc(rgb, 0.5 + 0.5 * math.sin(t * 2))


def fx_chase(c, r, t, rgb):
    return rgb if (xy_to_index(c, r) - int(t * 30)) % 10 < 3 else BLACK


def fx_fire(c, r, t, rgb):          # hottest at the floor (row 19)
    n = 0.5 + 0.5 * math.sin(c * 1.7 + r * 0.45 - t * 4.0 + 1.3 * math.sin(c * 0.9 + t * 1.7))
    heat = max(0.0, min(1.0, (r / (ROWS - 1)) * (0.6 + 0.8 * n) - 0.12)) * 3
    return (int(min(1, heat) * 255), int(max(0, min(1, heat - 1)) * 255), int(max(0, min(1, heat - 2)) * 255))


def fx_plasma(c, r, t, rgb):
    v = (math.sin(c * 0.7 + t) + math.sin(r * 0.35 - t * 1.3) + math.sin((c + r) * 0.4 + t * 0.7)) / 3
    return hsv(0.5 + 0.5 * v + t * 0.03, 1, 1)


def fx_aurora(c, r, t, rgb):
    v = 0.5 + 0.5 * math.sin(r * 0.35 + t * 0.8 + 2 * math.sin(c * 0.6 + t * 0.5))
    return hsv(0.35 + 0.25 * (0.5 + 0.5 * math.sin(c * 0.4 - t * 0.3)), 0.9, v * v)


def fx_rain(c, r, t, rgb):          # drops fall toward the floor, in the current colour
    speed, off = 6 + 7 * rnd(c + 1), rnd(c + 50) * 30
    d = ((t * speed + off) % (ROWS + 9)) - r
    if d < 0 or d >= 8:
        return BLACK
    return (255, 255, 255) if d < 1 else sc(rgb, (1 - d / 8) ** 1.5)


def fx_comet(c, r, t, rgb):         # runs along the snake, two comets half a lap apart
    i, best = xy_to_index(c, r), 0.0
    for k in (0, 1):
        head = (t * 45 + k * 100) % 230
        d = head - i
        if 0 <= d < 30:
            best = max(best, 1 - d / 30)
    return sc(rgb, best)


def fx_fireworks(c, r, t, rgb):
    total = [0.0, 0.0, 0.0]
    for k in range(3):
        n = int(t / 1.2) - k
        age = t - n * 1.2
        if n < 0 or age > 3.0:
            continue
        cx, cy = 1 + rnd(n) * 8, 3 + rnd(n + 77) * 11
        ring = abs(math.hypot(c - cx, r - cy) - age * 4.5)
        if ring < 1.2:
            col = hsv(rnd(n + 9), 1, 1)
            f = (1 - ring / 1.2) * (1 - age / 3.0) ** 1.5
            for j in range(3):
                total[j] += col[j] * f
    return tuple(min(255, int(x)) for x in total)


def fx_wave(c, r, t, rgb):
    return sc(rgb, 0.15 + 0.85 * (0.5 + 0.5 * math.sin(r * 0.45 - t * 3 + c * 0.25)))


FX = {"rainbow": fx_rainbow, "twinkle": fx_twinkle, "breathe": fx_breathe, "chase": fx_chase,
      "fire": fx_fire, "plasma": fx_plasma, "aurora": fx_aurora, "rain": fx_rain,
      "comet": fx_comet, "fireworks": fx_fireworks, "wave": fx_wave}


def render(buf, now):
    """Draw the current picture into buf. Returns True while it needs redrawing every frame."""
    if state["mode"] == "effect":
        name = state["effect"]
        if name == "solid":
            for c in range(COLS):
                for r in range(ROWS):
                    put(buf, c, r, state["rgb"])
            return False
        f, t = FX[name], now - state["t0"]
        for c in range(COLS):
            for r in range(ROWS):
                put(buf, c, r, f(c, r, t, state["rgb"]))
        return True

    if state["mode"] == "text":
        tx = state["text"]
        rgb = tx["rgb"] or state["rgb"]
        if tx["bg"]:
            for c in range(COLS):
                for r in range(ROWS):
                    put(buf, c, r, tx["bg"])
        s = (now - tx["t0"]) * tx["speed"]
        travel = tx["width"] + ROWS                  # text enters, crosses and leaves
        if tx["scroll"] and s >= travel:
            if tx["loop"]:
                s %= travel
            else:                                    # finished: back to a solid colour
                state.update(mode="effect", effect="solid", dirty=True)
                return False
        base = (ROWS - tx["width"]) // 2
        for p, gy in tx["lit"]:
            if tx["down"]:
                row = ROWS - int(s) + p if tx["scroll"] else base + p
                col = COLS - 1 - (tx["off"] + gy)
            else:
                row = int(s) - 1 - p if tx["scroll"] else ROWS - 1 - base - p
                col = tx["off"] + gy
            put(buf, col, row, rgb)
        return tx["scroll"]

    an = state["anim"]                               # mode == "frames"
    n = len(an["frames"])
    k = int((now - an["t0"]) * an["fps"])
    k = k % n if an["loop"] else min(k, n - 1)
    for c, r, rgb in an["frames"][k]:
        put(buf, c, r, rgb)
    return n > 1 and (an["loop"] or k < n - 1)


# ----------------------------------------------------------------- main
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
    print(f"Listening on '{TOPIC}', driving {LED_COUNT} LEDs (brightness cap {MAX_BRIGHTNESS}, "
          f"flip_x={FLIP_X}, flip_y={FLIP_Y})")
    shown = [None] * LED_COUNT
    shown_bri = None
    animated = False
    try:
        while True:
            m = consumer.poll(0.02 if animated else 0.2)
            if m is not None and not m.error():
                try:
                    apply(json.loads(m.value()))
                except Exception as e:        # a bad message must not kill the table
                    print("skipped message:", e)
            if not animated and not state["dirty"]:
                continue
            state["dirty"] = False
            buf = [BLACK] * LED_COUNT
            try:
                animated = render(buf, time.monotonic())
            except Exception as e:
                print("render error:", e)
                state.update(mode="effect", effect="solid")
                animated = False
                continue
            strip.setBrightness(state["bri"])
            changed = False
            for i, c in enumerate(buf):
                if c != shown[i]:
                    strip.setPixelColor(i, Color(*c))
                    shown[i] = c
                    changed = True
            if changed or state["bri"] != shown_bri:
                strip.show()
                shown_bri = state["bri"]
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
