#! /usr/bin/env bash
set -e

# Usage: set OUR_NODE and OUR_ADDRESS (host:port) to the node under test
if [ -z "$OUR_NODE" ] || [ -z "$OUR_ADDRESS" ]; then
    echo "Usage: OUR_NODE=<node> OUR_ADDRESS=<host:port> $0"
    exit 1
fi

# Run node 1999 as a hub with no radio, and let it find the node under test without DNS
sed -i "s|^rxchannel = SimpleUSB/1999.*|rxchannel = Local/pseudo|" /etc/asterisk/rpt.conf
sed -i "s|^1999 = radio@127.0.0.1/1999,NONE|&\n$OUR_NODE = radio@$OUR_ADDRESS/$OUR_NODE,NONE|" /etc/asterisk/rpt.conf

# Something for the node to transmit, as raw u-law
head -c 32000 /dev/urandom > /tmp/noise.ulaw

chown -R asterisk:asterisk /etc/asterisk /var/lib/asterisk /var/log/asterisk /var/spool/asterisk
exec asterisk -f -U asterisk -G asterisk -vvv
