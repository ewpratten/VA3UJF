# Build from the repository root: docker build -f subprojects/apps/va3ujf/Dockerfile .
FROM rust:1-slim-trixie AS planner
WORKDIR /usr/src/va3ujf

# The workspace lockfile covers every crate, so let cargo prune it to this one. That keeps the dependency layer cached across unrelated changes
COPY subprojects/apps/va3ujf/Cargo.toml ./
COPY Cargo.lock ./
RUN mkdir src && touch src/main.rs && cargo metadata --format-version 1 > /dev/null

FROM rust:1-slim-trixie AS builder
WORKDIR /usr/src/va3ujf

# Build dependencies in their own layer so source changes don't rebuild them
COPY --from=planner /usr/src/va3ujf/Cargo.toml /usr/src/va3ujf/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked && rm -rf src

COPY subprojects/apps/va3ujf/src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM gcr.io/distroless/cc-debian13
COPY --from=builder /usr/src/va3ujf/target/release/va3ujf /usr/local/bin/va3ujf

# Schedules and log timestamps follow the station's local time
ENV TZ=America/Toronto

# Runs as root to open the modem's serial device and write the host list cache
EXPOSE 8080/tcp 4569/udp
ENTRYPOINT ["va3ujf"]
