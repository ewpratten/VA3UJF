#! /usr/bin/env bash
set -e

# Usage: set GATEWAY_HOST, HOST_ADDRESS and HOST_PORT
for required in GATEWAY_HOST HOST_ADDRESS HOST_PORT; do
    if [ -z "${!required}" ]; then
        echo "Usage: GATEWAY_HOST=<host> HOST_ADDRESS=<host> HOST_PORT=<port> $0"
        exit 1
    fi
done

# What the gateway sends its repeater goes out to the test, and what the test sends comes in as if from the repeater
socat UDP4-LISTEN:20011,fork,reuseaddr UDP4:"$HOST_ADDRESS":"$HOST_PORT" &
exec socat UDP4-LISTEN:20012,fork,reuseaddr UDP4:"$GATEWAY_HOST":20010
