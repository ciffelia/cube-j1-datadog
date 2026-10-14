# cube-j1-datadog

A Go application that reads current smart meter measurements through a NextDrive Cube J1 every 10 seconds and sends them to Datadog.

## Metrics

| Metric | Description | Unit |
| --- | --- | --- |
| `cube_j1.power_watts` | Instantaneous power | W |
| `cube_j1.current_r_amperes` | Instantaneous current on the R phase | A |
| `cube_j1.current_t_amperes` | Instantaneous current on the T phase | A |
| `cube_j1.energy_forward_kwh` | Cumulative forward energy | kWh |
| `cube_j1.energy_reverse_kwh` | Cumulative reverse energy | kWh |

All metrics are sent as Datadog gauges with the tags `service:cube-j1-datadog` and `meter_uuid:<UUID>`. Energy readings are cumulative; use the difference between readings to calculate energy consumption over a period.

## Requirements

- Docker Engine and Docker Compose on Linux.
- A Cube J1 with its B-route connection to the smart meter already configured.
- Network access from the host to the Cube's MQTT over TLS endpoint and Datadog's HTTPS API.
- The smart meter UUID, MQTT password, and SHA-256 fingerprint of the Cube's TLS certificate.
- A Datadog site and API key.

Initial Cube setup and B-route registration are outside the scope of this application.

## Setup

Run the following commands in the project directory.

```sh
cp .env.example .env
```

Replace the sample values in `.env` with values for your environment.

| Variable | Description |
| --- | --- |
| `MQTT_BROKER` | MQTT endpoint using TLS, such as `ssl://192.0.2.10:8883`. Plaintext connections are not allowed. |
| `METER_UUID` | UUID of the connected smart meter |
| `MQTT_CERT_SHA256` | SHA-256 hash of the Cube's TLS certificate in DER format, expressed as 64 hexadecimal characters |
| `DD_SITE` | Datadog site, such as `datadoghq.com` or `us3.datadoghq.com` |

Store each credential in a separate file containing only its value. A trailing newline is allowed.

```sh
mkdir -p secrets
chmod 700 secrets
install -m 600 /path/to/mqtt-password secrets/mqtt-password
install -m 600 /path/to/datadog-api-key secrets/datadog-api-key
```

The container runs with UID/GID `1000:1000`. Set file ownership and permissions so that this UID can read the credential files. If necessary, adjust file ownership and the Compose `user` setting to match.

```sh
sudo docker compose up -d --build
sudo docker compose logs --tail=30 collector
```

The log messages `MQTT subscription active` and `Datadog accepted ... (HTTP 202)` confirm that the MQTT subscription succeeded and the Datadog API accepted the metrics.

## How it works

The application subscribes to the smart meter's `<UUID>/#` topics and sends an empty JSON object `{}` to each measurement topic to request a reading. Requests use QoS 1 and `retain=false`.

Each cycle sends four requests in sequence: power, current on both phases, cumulative forward energy, and cumulative reverse energy. A single current request returns both the R and T phase readings. The application waits for each request's responses before proceeding to the next. If the expected responses do not arrive within 20 seconds, the cycle ends; measurements already received remain eligible for sending. Missed cycles are skipped, and read cycles do not overlap.
