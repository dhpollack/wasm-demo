# Getting Started

## Download and Start Workspace

```bash
# clone repo
git clone https://github.com/weimeilin79/wasm-demo.git
cd  wasm-demo
# Start redpanda
docker-compose up -d
```

## Ingest Real-Time Data


```bash
# Setup cluster
rpk profile create workshop
rpk profile set kafka_api.brokers=localhost:19092
rpk profile set admin_api.addresses=localhost:19644
# Create topics
rpk topic create raw-data model-data -p 3
```

now produce some data

```bash
# todo
```


# Transforms

## Setup and enable data transforms

```bash
rpk cluster config set data_transforms_enabled true
docker compose down && docker compose up -d
```

initialize transform

```bash
rpk transform init superfast-panda-rs
```

## Write Code

todo

## Deploy To Redpanda

edit transforms.yaml

```bash
rpk transform deploy
```


# Build Model

```python
from keras.models import Sequential
from keras.layers import LSTM, Dense
from kafka import KafkaConsumer
import plotly.express as px
import numpy as np
import pandas as pd
import tensorflow as tf
import tensorflow_io as tfio

model = Sequential()
model.add(LSTM(128, return_sequences=True, input_shape= (3, 1)))
model.add(LSTM(64, return_sequences=False))
model.add(Dense(25))
model.add(Dense(1))
model.compile(optimizer='adam', loss='mean_squared_error', run_eagerly=True)
model.summary()

online_train_ds = tfio.experimental.streaming.KafkaBatchIODataset(
    topics=["model-data"],
    group_id="testzo",
    servers="redpanda-0:9092,redpanda-1:9092,redpanda-2:9092",
    stream_timeout=10000,
    configuration=[
        "session.timeout.ms=7000",
        "max.poll.interval.ms=8000",
        "auto.offset.reset=earliest"
    ],
)


def decode_kafka_online_item(raw_message, raw_key):
    message = tf.io.decode_csv(raw_message, [[0.0] for i in range(3)])
    key = tf.strings.to_number(raw_key)
    return (message, key)
  
batch_size = 20
for single_ds in online_train_ds:
    if len(single_ds) >= batch_size:
        single_ds = single_ds.shuffle(buffer_size=batch_size)
        single_ds = single_ds.map(decode_kafka_online_item)
        single_ds = single_ds.batch(batch_size)
    
        model.fit(single_ds, epochs=1)
        tf.keras.models.save_model(model, "./time_prediction_model")
    else:
        print("Not enough data in the dataset. Skipping model fitting.")
```
