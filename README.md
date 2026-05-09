# Tims smart meter script

Reads DSMR smart meter data from the P1 serial port

## Features

- Obviously takes your smart meter data and sends it to InfluxDB and MQTT (see below)
- Also computes statistics, and publishes to InfluxDB and MQTT
- Keeps state for statistics across restarts
- Calculates current through neutral just for fun (note that the real current might be greater due to harmonics)
- Verifies CRC for DSMR versions that use it
- Supports Home Assistant auto-discovery

## Components

| File | Purpose |
|---|---|
| `smartmeter.py` | Main daemon |
| `smartmeter.service` | systemd unit |
| `smartmeter-config.yaml` | Configuration |
| `mqtt2dsmr.sh` | Bridge: MQTT telegram topic → TCP socket (port 5555) |
| `voltagemonitord.sh` | Voltage threshold monitor with Pushover alerts |
| `voltagemonitord.conf` | Configuration for the voltage monitor |

## Requirements

```
pip install paho-mqtt influxdb pyserial pyyaml libscrc
```

When running as a daemon, create the smartmeter user and make sure the serial port device (e.g. `/dev/ttyUSB0`) is readable by the `smartmeter` user.

## Configuration

Copy `smartmeter-config.yaml` to `/etc/smartmeter/smartmeter-config.yaml` and fill in credentials:

```yaml
serialport: '/dev/ttyUSB0'
dsmrversion: 5          # 5, 4, or 2.2

influxdb:
  host:     "localhost"
  port:     8086
  username: "yourusername"
  password: "yourpassword"
  database: "smartmeter"

mqtt:
  host:     "localhost"
  username: "yourusername"
  password: "yourpassword"
  topic:    "smartmeter"

ha_discovery_prefix: "homeassistant"
```

`dsmrversion` controls the serial baud rate (DSMR 5 → 115200, older → 9600) and the sliding-average window (5 min = 300 samples at 1 s, or 30 samples at 10 s).

## Installation

Create the user, directories, and state file:

```bash
useradd --system --no-create-home --shell /usr/sbin/nologin smartmeter
usermod -aG dialout smartmeter   # grant access to the serial port

mkdir -p /usr/local/share/homeautomation
mkdir -p /etc/smartmeter
touch /var/local/smartmeterstate.json
chown smartmeter:smartmeter /var/local/smartmeterstate.json
```

Install the script and service:

```bash
cp smartmeter.py /usr/local/share/homeautomation/smartmeter.py
chmod +x /usr/local/share/homeautomation/smartmeter.py
cp smartmeter.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now smartmeter
```

The service runs as the `smartmeter` user and depends on `mosquitto.service`. It restarts automatically on failure (5 s delay).

Reload configuration without restarting:
```bash
systemctl reload smartmeter   # sends SIGHUP
```

## Runtime files

| Path | Contents |
|---|---|
| `/etc/smartmeter/smartmeter-config.yaml` | Configuration |
| `/dev/shm/smartmeter.json` | Latest parsed telegram + stats (in-memory) |
| `/var/local/smartmeterstate.json` | Persistent state: daily totals, midnight snapshots |

State is written to disk so daily totals survive restarts.

## What it publishes

**MQTT** — topics under the configured base topic, e.g. `smartmeter/<meter-id>/`:
- Per-phase voltage, current, power
- Electricity tariff 1 / 2 delivered and returned
- 5-minute average power
- Daily and yesterday electricity usage (t1 + t2)
- Gas delivery and flow rate
- Raw telegram

**InfluxDB** — measurements in the configured database, including all of the above plus derived statistics.

**Home Assistant** — MQTT discovery messages published to `homeassistant/sensor/...` on startup, so sensors appear automatically in HA.

## Voltage monitor

`voltagemonitord.sh` subscribes to the MQTT voltage topics and sends a Pushover notification when any phase goes below 207 V or above 253 V (configurable). Alerts are debounced by 60 seconds.

Configure `/etc/voltagemonitord/voltagemonitord.conf`:
```
LOW_THRESHOLD=207
HIGH_THRESHOLD=253
MQTT_HOST=localhost
METER_ID=E0067005564809720
PUSHOVER_TOKEN=your_app_token
PUSHOVER_USER=your_user_key
```

## mqtt2dsmr bridge

`mqtt2dsmr.sh` is a lightweight bridge for situations where the meter is on a different machine. It subscribes to the raw telegram topic over MQTT and re-exposes it as a TCP stream on port 5555, so `smartmeter.py` can connect to it using a TCP serial device (`/dev/tcp/host/5555` or `socat`).
This is useful when you want to emulate the smart meter again on some other machine.
