# A real AllStarLink 3 node (app_rpt on Asterisk) to link against, without the DAHDI kernel modules the stock container needs
FROM debian:13-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends wget ca-certificates \
    && wget -q https://repo.allstarlink.org/public/asl-apt-repos.deb13_all.deb \
    && dpkg -i asl-apt-repos.deb13_all.deb \
    && apt-get update \
    && apt-get install -y --no-install-recommends asl3-asterisk asl3-asterisk-config \
    && rm -rf /var/lib/apt/lists/* asl-apt-repos.deb13_all.deb
COPY start.sh /usr/local/bin/start.sh
ENTRYPOINT ["/usr/local/bin/start.sh"]
