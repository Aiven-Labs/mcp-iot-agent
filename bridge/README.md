# Table lights controller (runs on the Pi)

`table_lights.py` consumes the `table-lights` Kafka topic and drives the table's LED strip directly
with `rpi_ws281x`, on the same pin and wiring as `tetris_led.py`. It only makes outbound
connections, so no ports need opening.

**The game and this controller share the LEDs, so run one or the other.** Both use the lock file
`/tmp/tetris_led.lock`; whichever starts second refuses to start.

## Setup

1. In the Aiven Console, download `ca.pem`, `service.cert` and `service.key` from the Kafka
   service's overview page.
2. On the Pi:
   ```
   sudo mkdir -p /opt/table-lights && sudo chown $USER /opt/table-lights
   cp table_lights.py requirements.txt /opt/table-lights/
   cp ca.pem service.cert service.key /opt/table-lights/
   chmod 600 /opt/table-lights/service.key
   cd /opt/table-lights
   sudo python3 -m venv --system-site-packages .venv    # sees the rpi_ws281x the game already uses
   sudo .venv/bin/pip install -r requirements.txt
   ```
3. Create `/opt/table-lights/.env` from the Pi section of `.env.example`.
4. Try it by hand (needs root for the LEDs):
   ```
   sudo bash -c 'set -a; . /opt/table-lights/.env; set +a; .venv/bin/python table_lights.py'
   ```
5. To run it at boot: copy `table-lights.service` to `/etc/systemd/system/`, then
   `sudo systemctl enable --now table-lights`.

## Playing the game

```
sudo systemctl stop table-lights      # give the LEDs back
sudo python3 tetris_led.py
sudo systemctl start table-lights     # afterwards
```

## What it can show

- Solid colours and brightness, plus built-in effects: solid, rainbow, twinkle, breathe, chase, fire,
  plasma, aurora, rain, comet, fireworks, wave.
- **Scrolling text** (`show_text`), using the game's 3x5 font. The table is 10 wide x 20 tall, so text is
  rotated to run along the long side and the letters fill the width. Tilt your head right to read it
  (`direction: "down"`), or left for `"up"`.
- **Freeform pictures and animations** (`draw`, `animate`): the agent sends up to 40 frames of 20 rows x 10
  characters with a colour code per LED.

Pictures use "row 0 = the end where game pieces spawn". If a picture or text is mirrored or upside down for
your table, either set `FLIP_X=1` / `FLIP_Y=1` in `.env`, or point `TETRIS_ORIENT_FILE` at the game's
`tetris_orientation.json` so both agree.

## Notes

- Brightness is capped by `MAX_BRIGHTNESS` (default 200) so an agent can't draw the full current of
  every LED in white. Raise it only if your power supply is sized for it.
- `set_color` always switches back to solid. Anything past the first 200 LEDs (the side panel) stays dark.
- Commands older than the controller's start are skipped (`auto.offset.reset=latest`).
