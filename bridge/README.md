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

## Notes

- Brightness is capped by `MAX_BRIGHTNESS` (default 200) so an agent can't draw the full current of
  every LED in white. Raise it only if your power supply is sized for it.
- Effects: solid, rainbow, twinkle, breathe, chase. `set_color` always switches back to solid.
- Commands older than the controller's start are skipped (`auto.offset.reset=latest`).
