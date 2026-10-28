#! /usr/bin/env bash
set -e

# Usage: set GATEWAY_CALLSIGN, IRCDDB_HOST, REPEATER_ADDRESS and REPEATER_PORT
for required in GATEWAY_CALLSIGN IRCDDB_HOST REPEATER_ADDRESS REPEATER_PORT; do
    if [ -z "${!required}" ]; then
        echo "Usage: GATEWAY_CALLSIGN=<call> IRCDDB_HOST=<host> REPEATER_ADDRESS=<host> REPEATER_PORT=<port> $0"
        exit 1
    fi
done

# The gateway won't start without a local MQTT broker
printf 'listener 1883 127.0.0.1\nallow_anonymous true\n' > /tmp/mosquitto.conf
mosquitto -d -c /tmp/mosquitto.conf

# The gateway only takes a repeater address that's short enough to be an IP address
REPEATER_ADDRESS=$(getent ahostsv4 "$REPEATER_ADDRESS" | awk 'NR==1 {print $1}')
export REPEATER_ADDRESS

envsubst < /etc/DStarGateway.ini.template > /etc/DStarGateway.ini
exec dstargateway /etc/DStarGateway.ini
