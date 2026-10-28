# Build from the repository root: docker build .
FROM rust:1-slim-trixie AS builder

# The speech engine is a static C++ library, so linking needs a C++ toolchain
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/va3ujf

# Build dependencies in their own layer so source changes don't rebuild them
COPY Cargo.toml ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release

# The voice announcements are spoken in, with its checksum, so a changed download can't slip in
FROM debian:trixie-slim AS voice
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl bzip2 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/vits-piper-en_US-lessac-medium-int8.tar.bz2 -o voice.tar.bz2 \
    && echo "f1c6d0295cf16087b05f80fdca5b44daca5cd78e2c425d419a42ba34929805f9  voice.tar.bz2" | sha256sum -c - \
    && mkdir /voice && tar -xjf voice.tar.bz2 --strip-components=1 -C /voice

FROM gcr.io/distroless/cc-debian13
COPY --from=builder /usr/src/va3ujf/target/release/va3ujf /usr/local/bin/va3ujf
COPY --from=voice /voice /usr/share/va3ujf/voice

# Schedules and log timestamps follow the station's local time
ENV TZ=America/Toronto

# Runs as root to open the modem's serial device and write the host list cache
EXPOSE 8080/tcp 4569/udp
ENTRYPOINT ["va3ujf"]
