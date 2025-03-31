docker_compose := if os() == "linux" { "docker compose" } else { "docker-compose" }
export CPUARCH := if os() == "linux" { "x86_64" } else { "aarch64" }

# list of commands
help:
    just -u -l

# ensure model.safetensors exists and wasm32-wasip1 is installed
setup:
    touch delivery-model-rs/model.safetensors
    rustup target add wasm32-wasip1
    rpk profile create foodtime-rs
    rpk profile set kafka_api.brokers=localhost:19092
    rpk profile set admin_api.addresses=localhost:19644

# bring up redpanda cluster
docker-redpanda-up:
    {{ docker_compose }} up -d

# bring down redpanda cluster
docker-redpanda-down *args:
    {{ docker_compose }} down {{ args }}

# build and deploy transform (haversine distance)
[working-directory: 'foodtime-rs']
build-deploy-transform:
    rpk transform build
    rpk transform deploy

# build and deploy transform (h3 grid distance)
[working-directory: 'foodtime-rs']
build-deploy-transform-h3:
    rpk transform build -- --features h3
    rpk transform deploy --var DIST_CALC_TYPE=h3

# run producer
[working-directory: 'delivery-producer-rs']
run-producer:
    cargo run --release

[working-directory: 'delivery-model-rs']
_run-model *args:
    cargo run --release {{ args }}

_run-train *args: (_run-model "--bin" "train" args)

# run local training
run-train-local: (_run-train "--" "-m" "traditional")

# run streaming training
run-train-streaming: (_run-train "--" "-m" "streaming")

# run inference web server
run-inference-web: (_run-model "--bin" "inference-web")

# build and run producer, streaming training, and inference server via docker compose
docker-streaming-up:
    {{ docker_compose }} -f docker-compose.services.yaml up --build

# run load-test with k6
load-test *args:
    k6 run delivery-model-rs/test/k6/script.js {{ args }}

# send dummy request
post-request:
    jq -nc '{"age": 25, "dist": 19.0, "rating": 4.5, "order_type": 1, "vehicle_type": 2}' | xh localhost:8000

# reload model in inference server
get-reload:
    xh localhost:8000/reload
