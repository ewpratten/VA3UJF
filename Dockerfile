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
RUN curl -fsSL https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/vits-piper-en_US-lessac-low.tar.bz2 -o voice.tar.bz2 \
    && echo "8fb427b8637334072ee5723d72fa418c45bfdd4b7deebeacdf2938662618c1cb  voice.tar.bz2" | sha256sum -c - \
    && mkdir /voice && tar -xjf voice.tar.bz2 --strip-components=1 -C /voice

# The speech recognition model voice control transcribes with, checked the same way
FROM debian:trixie-slim AS stt
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl bzip2 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-moonshine-base-en-quantized-2026-02-27.tar.bz2 -o stt.tar.bz2 \
    && echo "43232c1d13013d37317163baec3135bd771a186a4356f28c889bab453bb0e891  stt.tar.bz2" | sha256sum -c - \
    && mkdir /stt && tar -xjf stt.tar.bz2 --strip-components=1 -C /stt --exclude=test_wavs

FROM gcr.io/distroless/cc-debian13
COPY --from=builder /usr/src/va3ujf/target/release/va3ujf /usr/local/bin/va3ujf
COPY --from=voice /voice /usr/share/va3ujf/voice
COPY --from=stt /stt /usr/share/va3ujf/stt

# Schedules and log timestamps follow the station's local time
ENV TZ=America/Toronto

# Runs as root to open the modem's serial device and write the host list cache
EXPOSE 8080/tcp 4569/udp
ENTRYPOINT ["va3ujf"]
