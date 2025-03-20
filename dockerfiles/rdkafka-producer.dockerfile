FROM lukemathwalker/cargo-chef:latest-rust-1.82.0 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN apt update && apt install -y cmake
RUN cargo chef cook --release --recipe-path recipe.json

COPY Cargo.lock Cargo.toml ./
COPY src ./src/
RUN cargo build --release

# Bundle Stage
FROM gcr.io/distroless/cc
ARG CPUARCH

COPY --from=builder /app/target/release/rdkafka-producer /rdkafka-producer
COPY --from=builder /usr/lib/${CPUARCH}-linux-gnu/libz.so* /usr/lib/${CPUARCH}-linux-gnu/
COPY deliverytime.txt /deliverytime.txt
USER 1000
CMD ["/rdkafka-producer"]

