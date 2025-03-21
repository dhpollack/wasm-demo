
# Red Panda + Rust - Real Time Data Transforms and Model Training

This is an extension of the [Red Panda Data Transforms tutorial](https://docs.redpanda.com/redpanda-labs/?q=getting%20started%20with%20data%20transforms) in the Red Panda Labs.  It takes the toy example of doing a data transform in Golang and modeling in Python and does the exact same thing except in Rust.  Classic rust rewrite.

I recommend you do the actual tutorial since it's pretty good and gives you an idea of what this should do. (note: I had to upgrade the redpanda images to get it to work with the version of rpk in the tutorial image).

## Requirements

- [Rust](https://www.rust-lang.org/)
- [RedPanda CLI (rpk)](https://docs.redpanda.com/current/get-started/rpk/)
- docker + docker compose
- CMake (for [rust-rdkafka](https://github.com/fede1024/rust-rdkafka))
- jq & xh (you can use echo and curl but I like these tools more)

## Instructions

### Before Getting Started

You may need to use `docker-compose` instead of `docker compose` to run the docker commands.

### Clone Repo and Start Red Panda

```bash
# clone repo
git clone https://github.com/dhpollack/wasm-demo.git && cd wasm-demo
git checkout wasm-demo-rs
# Start redpanda
docker-compose up -d
```

### Setup Cluster

First we need to setup up the redpanda cli and cluster.

```bash
# Setup your cli
rpk profile create foodtime-rs
rpk profile set kafka_api.brokers=localhost:19092
rpk profile set admin_api.addresses=localhost:19644
# Create topics in cluster
rpk topic create raw-data training-data -p 3
```

### Produce some data

Now let's check if the cluster is working...

```bash
cd delivery-producer-rs
cargo run --release
cd -
```

### Setup Transforms

The original tutorial creates a data transform in tinygo that gets compiled to wasm.  We are going to do the same thing in Rust.

```bash
rpk cluster config set data_transforms_enabled true
docker compose down && docker compose up -d
cd foodtime-rs
rpk transform build
rpk transform deploy
cd -
```

### Train Model in Real-Time

We are going to produce an infinite stream of random rows from the training data and put them into the redpanda stream.  Then in another terminal we will read from this stream and train our model in an online manner.  This is similar to training a normal neural network except we have an infinite number of epochs, so we will save checkpoints every 10 epochs.

```bash
# terminal 1
cd delivery-producer-rs
cargo run --release
# terminal 2
cd delivery-model-rs
cargo run --release --bin train
```

You should keep the above terminals open and running for the next step.

### Classic Inference Server

Next we will do inference with our trained model.  Will see that the model gives the same response as long as we do not reload the model.  Once we reload the model, the response will change.  Feel free to change the request and reload the model as much as you want.

```bash
# terminal 3
cd delivery-model-rs
cargo run --release --bin inference-web
# terminal 4
# send request (gives a deterministic response until model is reloaded)
jq -nc '{"age": 25, "dist": 19.0, "rating": 4.5}' | xh localhost:8000
# reload model
xh localhost:8000/reload
```

You should see something like this:

```bash
[david@fedora-4 wasm-demo]$ gojq -nc '{"age": 25.4, "dist": 15.5, "rating": 4.1}' | xh localhost:8000
HTTP/1.1 200 OK
Content-Length: 27
Content-Type: application/json
Date: Thu, 20 Mar 2025 21:34:57 GMT
Permissions-Policy: interest-cohort=()
Server: Rocket
X-Content-Type-Options: nosniff
X-Frame-Options: SAMEORIGIN

{
    "delivery_time": 3.5620039
}


[david@fedora-4 wasm-demo]$ xh localhost:8000/reload
HTTP/1.1 200 OK
Content-Length: 0
Date: Thu, 20 Mar 2025 21:35:10 GMT
Permissions-Policy: interest-cohort=()
Server: Rocket
X-Content-Type-Options: nosniff
X-Frame-Options: SAMEORIGIN


[david@fedora-4 wasm-demo]$ gojq -nc '{"age": 25.4, "dist": 15.5, "rating": 4.1}' | xh localhost:8000
HTTP/1.1 200 OK
Content-Length: 27
Content-Type: application/json
Date: Thu, 20 Mar 2025 21:35:12 GMT
Permissions-Policy: interest-cohort=()
Server: Rocket
X-Content-Type-Options: nosniff
X-Frame-Options: SAMEORIGIN

{
    "delivery_time": 22.482447
}
```

The exact results will differ depending on how long you trained the model.

## Docker Compose Version

First complete the steps until `Setup Transforms`.

Next build and run all the services with docker compose:

```bash
# if you are running on a arm-based mac then uncomment the next line
# export CPUARCH=aarch64
docker compose -f docker-compose.services.yaml build
docker compose -f docker-compose.services.yaml up
```


# Reference

- [Original Tutorial on Red Panda Labs](https://docs.redpanda.com/redpanda-labs/?q=getting%20started%20with%20data%20transforms)
- https://thecleverprogrammer.com/2023/01/02/food-delivery-time-prediction-using-python/

