# Stands in for the repeater's address on a container network, passing datagrams between the gateway and a test on the host
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends socat && rm -rf /var/lib/apt/lists/*
COPY start.sh /usr/local/bin/start.sh
EXPOSE 20012/udp
ENTRYPOINT ["/usr/local/bin/start.sh"]
