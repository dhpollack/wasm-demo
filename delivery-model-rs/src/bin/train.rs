// This should reach 91.5% accuracy.
#[cfg(feature = "mkl")]
extern crate intel_mkl_src;

#[cfg(feature = "accelerate")]
extern crate accelerate_src;

use std::fs::File;
use std::io::{self, BufRead};

use futures::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::sync::{
    mpsc::{self, channel},
    oneshot,
};
use tokio::time::{sleep, Duration};

use candle_core::{DType, Device, Tensor};
use candle_nn::{loss, Optimizer, VarBuilder, VarMap};

use delivery_model::{
    data::{Dataset, TrainingItem},
    model::LinearModel,
};

const INPUT_DIM: usize = 3;
const OUTPUT_DIM: usize = 1;
const LEARNING_RATE: f64 = 0.002;
const BATCH_SIZE: usize = 100;

fn load_data(dev: &Device) -> anyhow::Result<Dataset> {
    let file = File::open("dataset.jsonl")?;
    let reader = io::BufReader::new(file);
    let (data_vec, labels_vec): (Vec<f32>, Vec<f32>) = reader
        .lines()
        .map(|line| {
            let l = line.expect("unable to parse line");
            let item: TrainingItem =
                serde_json::from_str(&l).expect("unable to serialize TrainingItem");
            item
        })
        .fold((vec![], vec![]), |(mut td, mut tl), item| {
            td.push(item.req.age);
            td.push(item.req.dist);
            td.push(item.req.rating);
            tl.push(item.resp.delivery_time);
            (td, tl)
        });
    let data_tensor = Tensor::from_vec(data_vec.clone(), (data_vec.len() / 3, 3), dev)?;
    let labels_tensor = Tensor::from_vec(labels_vec.clone(), labels_vec.len(), dev)?;
    Ok(Dataset {
        train_data: data_tensor,
        train_labels: labels_tensor,
    })
}

fn train_from_local(dev: &Device) -> anyhow::Result<LinearModel> {
    let m = load_data(dev)?;
    let train_data = m.train_data.to_device(dev)?;
    let train_labels = m.train_labels.to_device(dev)?;
    let varmap = VarMap::new();
    let vs = VarBuilder::from_varmap(&varmap, DType::F32, dev);
    let model = LinearModel::new(INPUT_DIM, OUTPUT_DIM, vs.clone())?;
    let mut opt = candle_nn::AdamW::new(
        varmap.all_vars(),
        candle_nn::ParamsAdamW {
            lr: LEARNING_RATE,
            ..Default::default()
        },
    )?;
    for _ in 1..200 {
        let preds = model.forward(&train_data)?.squeeze(1)?;
        let epoch_loss = loss::mse(&preds, &train_labels)?;
        opt.backward_step(&epoch_loss)?;

        println!("Train Loss: {:8.5}", epoch_loss.to_scalar::<f32>()?)
    }

    varmap.save("model.safetensors")?;

    Ok(model)
}

async fn create_consumer<'a>(topic: &'a str, brokers: &'a str) -> anyhow::Result<StreamConsumer> {
    println!("Create producer...");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("group.id", "rust-rdkafka-roundtrip-example")
        .create()
        .expect("Consumer creation failed");
    consumer.subscribe(&[topic]).unwrap();
    Ok(consumer)
}

async fn training_item_stream(sender: mpsc::Sender<TrainingItem>) -> anyhow::Result<()> {
    // Setup kafka consumer and put stream messages into channel
    let topic = "training-data";
    let brokers = "0.0.0.0:19092,0.0.0.0:29092,0.0.0.0:39092";
    let consumer = create_consumer(topic, brokers).await?;
    let mut stream = consumer.stream();
    while let Some(Ok(borrowed_message)) = stream.next().await {
        let owned_message = borrowed_message.detach();
        let payload = owned_message.payload().unwrap();
        //println!("payload: {}", String::from_utf8_lossy(payload));
        let training_sample: TrainingItem =
            serde_json::from_slice(payload).expect("unable able to deserialize TrainingItem");
        if let Err(err) = sender.send(training_sample).await {
            println!("tx send error: {}", err);
            break;
        }
    }
    Ok(())
}

async fn train_from_redpanda(mut receiver: mpsc::Receiver<TrainingItem>) -> anyhow::Result<()> {
    // Setup dummy receivers
    let (_tx, mut rx) = oneshot::channel::<()>();
    // Setup model
    let dev = Device::cuda_if_available(0)?;
    let varmap = VarMap::new();
    let vs = VarBuilder::from_varmap(&varmap, DType::F32, &dev);
    let model = LinearModel::new(INPUT_DIM, OUTPUT_DIM, vs.clone())?;
    let mut opt = candle_nn::AdamW::new(
        varmap.all_vars(),
        candle_nn::ParamsAdamW {
            lr: LEARNING_RATE,
            ..Default::default()
        },
    )?;
    let mut epoch: usize = 0;
    loop {
        let mut items: Vec<TrainingItem> = vec![];
        tokio::select! {
            _ = tokio::time::timeout(Duration::from_secs(10), &mut rx) => {
                println!("STOP RECEIVING");
                return Ok(());
            }
            num_received = receiver.recv_many(&mut items, BATCH_SIZE) => {
                epoch += 1;
                println!("num received: {num_received}");
                let (data_vec, labels_vec): (Vec<f32>, Vec<f32>) =
                    items
                        .into_iter()
                        .fold((vec![], vec![]), |(mut td, mut tl), item| {
                            td.push(item.req.age);
                            td.push(item.req.dist);
                            td.push(item.req.rating);
                            tl.push(item.resp.delivery_time);
                            (td, tl)
                        });
                let data_tensor = Tensor::from_vec(data_vec.clone(), (data_vec.len() / 3, 3), &dev)?;
                let labels_tensor = Tensor::from_vec(labels_vec.clone(), labels_vec.len(), &dev)?;
                let m = Dataset {
                    train_data: data_tensor,
                    train_labels: labels_tensor,
                };
                let train_data = m.train_data.to_device(&dev)?;
                let train_labels = m.train_labels.to_device(&dev)?;
                let preds = model.forward(&train_data)?.squeeze(1)?;
                let epoch_loss = loss::mse(&preds, &train_labels)?;
                opt.backward_step(&epoch_loss)?;

                println!("Train Loss: {:8.5}", epoch_loss.to_scalar::<f32>()?);
                if epoch % 10 == 0 {
                    println!("Saving model after {epoch} epochs");
                    varmap.save("model.safetensors")?;
                }
            }
        }
        sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dev = Device::cuda_if_available(0)?;
    let from_redpanda = true;

    if from_redpanda {
        // Setup channels
        let (tx, rx) = channel::<TrainingItem>(BATCH_SIZE);
        let t1 = tokio::spawn(training_item_stream(tx));
        let t2 = tokio::spawn(train_from_redpanda(rx));
        let (_, _) = (t1.await??, t2.await??);
    } else {
        match train_from_local(&dev) {
            Ok(_) => {}
            Err(e) => {
                println!("Error: {}", e);
                return Err(e);
            }
        };
    }

    Ok(())
}
