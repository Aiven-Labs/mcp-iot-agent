# WLED bridge

Runs on your home network. It consumes the `table-lights` Kafka topic and
POSTs each command to WLED's `/json/state`. It only makes outbound
connections, so no ports need opening.

## Setup

1. Download `ca.pem`, `service.cert` and `service.key` from the Kafka
   service's overview page in the Aiven Console.
2. Copy `.env.example` (repo root) to `.env` and set the bridge values:
   `WLED_IP`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CA_FILE`, `KAFKA_CERT_FILE`,
   `KAFKA_KEY_FILE`.
3. Install and run:

```
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
set -a; . ./.env; set +a
.venv/bin/python bridge.py
```

To keep it running across reboots on a Raspberry Pi, copy the files to
`/opt/wled-bridge`, then install `wled-bridge.service` into
`/etc/systemd/system/` and run `sudo systemctl enable --now wled-bridge`.

Commands older than the bridge's start time are skipped
(`auto.offset.reset=latest`), so a restart never replays old light changes.
