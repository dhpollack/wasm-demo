
# Red Panda + Rust - Real Time Data Transforms and Model Training

This is an extension of the [Red Panda Data Transforms tutorial](https://docs.redpanda.com/redpanda-labs/?q=getting%20started%20with%20data%20transforms) in the Red Panda Labs.  It takes the toy example of doing a data transform in golang and model training in python and does everything in rust.  Classic rust rewrite.

I recommend you do the actual tutorial since it's pretty good and gives you an idea of what this should do. (note: I had to upgrade the redpanda images to get it to work with the version of rpk in the tutorial image)

## Requirements

- [Rust](https://www.rust-lang.org/)
- [RedPanda CLI (rpk)](https://docs.redpanda.com/current/get-started/rpk/)
- docker + docker compose
- CMake (for [rust-rdkafka](https://github.com/fede1024/rust-rdkafka))

## Instructions

### Before Getting Started

You may need to use `docker-compose` instead of `docker compose` to run the docker commands.

### Clone Repo and Start Red Panda

```bash
# clone repo
git clone https://github.com/dhpollack/wasm-demo.git
cd  wasm-demo
# Start redpanda
docker-compose up -d
```

### Setup Cluster

```bash
# Setup cluster
rpk profile create foodtime-rs
rpk profile set kafka_api.brokers=localhost:19092
rpk profile set admin_api.addresses=localhost:19644
# Create topics
rpk topic create raw-data training-data -p 3
```

### Produce some data

```bash
cd delivery-producer-rs
cargo run --release
cd -
```

### Setup Transforms

```bash
rpk cluster config set data_transforms_enabled true
docker compose down && docker compose up -d
cd foodtime-rs
rpk transform build
rpk transform deploy
cd -
```

### Train Model in Real-Time

```bash
# terminal 1
cd delivery-producer-rs
cargo run --release
# terminal 2
cd delivery-model-rs
cargo run --release
```

# Reference

- [Original Tutorial on Red Panda Labs](https://docs.redpanda.com/redpanda-labs/?q=getting%20started%20with%20data%20transforms)
- https://thecleverprogrammer.com/2023/01/02/food-delivery-time-prediction-using-python/

