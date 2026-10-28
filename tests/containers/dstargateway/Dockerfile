# The real DStarGateway, built from the same revision as the mmdvm stack, with the MQTT broker it insists on
FROM debian:trixie-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git g++ make libmosquitto-dev nlohmann-json3-dev libboost-dev
WORKDIR /src
ARG DSTARGATEWAY_REV=612f388727a9bb47aaeaae3a89f5abff3152ed93
RUN git init --quiet && git fetch --depth 1 https://github.com/g4klx/DStarGateway.git ${DSTARGATEWAY_REV} && git checkout --quiet FETCH_HEAD && make -j$(nproc)

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base libmosquitto1 mosquitto && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/DStarGateway/dstargateway /usr/local/bin/
COPY --from=build /src/Data/*.ambe /src/Data/*.indx /src/Data/*.json /usr/local/share/dstargateway.d/
COPY --from=build /src/Data/DStar_Hosts.json /var/lib/dstargateway/
COPY DStarGateway.ini.template /etc/DStarGateway.ini.template
COPY start.sh /usr/local/bin/start.sh
EXPOSE 30001/udp 20010/udp
ENTRYPOINT ["/usr/local/bin/start.sh"]
