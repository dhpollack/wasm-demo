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

COPY --from=builder /app/target/release/train /train
COPY --from=builder /usr/lib/x86_64-linux-gnu/libz.so* /usr/lib/x86_64-linux-gnu/
USER 1000
CMD ["/train"]

