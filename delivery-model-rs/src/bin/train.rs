// This should reach 91.5% accuracy.
#[cfg(feature = "mkl")]
extern crate intel_mkl_src;

#[cfg(feature = "accelerate")]
extern crate accelerate_src;

use std::fs::File;
use std::io::{self, BufRead};

use clap::{Parser, ValueEnum};
use delivery_model::model::CategoricalEmbeddings;
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
    data::{Dataset, TrainData, TrainingItem},
    model::LinearModel,
    settings::Settings,
};

/// A program to train a model traditionally or with online with streaming data
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Model training mode
    #[arg(short, long, value_enum, default_value_t = TrainingMode::Traditional)]
    mode: TrainingMode,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum TrainingMode {
    /// Traditional training from a dataset file
    Traditional,
    /// Streaming training from a redpanda bus
    Streaming,
}

fn load_data(dev: &Device) -> anyhow::Result<Dataset> {
    let file = File::open("dataset.jsonl")?;
    let reader = io::BufReader::new(file);
    let (data_vec, categorical_vec, labels_vec): (Vec<f32>, Vec<u32>, Vec<f32>) = reader
        .lines()
        .map(|line| {
            let l = line.expect("unable to parse line");
            let item: TrainingItem =
                serde_json::from_str(&l).expect("unable to serialize TrainingItem");
            item
        })
        .fold(
            (vec![], vec![], vec![]),
            |(mut td, mut temb, mut tl), item| {
                // build non-categorical features
                td.push(item.req.age);
                td.push(item.req.dist);
                td.push(item.req.rating);
                // build categorical features
                temb.push(item.req.order_type);
                temb.push(item.req.vehicle_type);
                // build labels
                tl.push(item.resp.delivery_time);
                (td, temb, tl)
            },
        );
    let num_items = labels_vec.len();
    let data_tensor = Tensor::from_vec(data_vec, (num_items, 3), dev)?;
    let categorical_tensor = Tensor::from_vec(categorical_vec, (num_items, 2), dev)?;
    let labels_tensor = Tensor::from_vec(labels_vec, num_items, dev)?;
    Ok(Dataset {
        train_data: TrainData {
            features: data_tensor,
            categories: categorical_tensor,
        },
        train_labels: labels_tensor,
    })
}

fn train_from_local(
    input_dim: usize,
    output_dim: usize,
    learning_rate: f64,
    weights_path: &str,
    dev: &Device,
) -> anyhow::Result<LinearModel> {
    let m = load_data(dev)?;
    let features_data = m.train_data.features.to_device(dev)?;
    let categorical_data = m.train_data.categories.to_device(dev)?;
    let train_labels = m.train_labels.to_device(dev)?;
    let varmap = VarMap::new();
    let vs = VarBuilder::from_varmap(&varmap, DType::F32, dev);
    let embedding_model = CategoricalEmbeddings::new(5, 5, vs.clone())?;
    let model = LinearModel::new(input_dim, output_dim, vs.clone())?;
    let mut opt = candle_nn::AdamW::new(
        varmap.all_vars(),
        candle_nn::ParamsAdamW {
            lr: learning_rate,
            ..Default::default()
        },
    )?;
    for _ in 1..200 {
        let embeddings = embedding_model.forward(&categorical_data)?;
        let train_data = Tensor::cat(&[&features_data, &embeddings], 1)?;
        let preds = model.forward(&train_data)?.squeeze(1)?;
        let epoch_loss = loss::mse(&preds, &train_labels)?;
        println!("Train Loss: {:8.5}", epoch_loss.to_scalar::<f32>()?);
        opt.backward_step(&epoch_loss)?;
    }

    varmap.save(weights_path)?;

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

async fn training_item_stream(
    topic: &str,
    brokers: &str,
    sender: mpsc::Sender<TrainingItem>,
) -> anyhow::Result<()> {
    // Setup kafka consumer and put stream messages into channel
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

async fn train_from_redpanda(
    input_dim: usize,
    output_dim: usize,
    learning_rate: f64,
    batch_size: usize,
    weights_path: &str,
    mut receiver: mpsc::Receiver<TrainingItem>,
) -> anyhow::Result<()> {
    // Setup dummy receivers
    let (_tx, mut rx) = oneshot::channel::<()>();
    // Setup model
    let dev = Device::cuda_if_available(0)?;
    let varmap = VarMap::new();
    let vs = VarBuilder::from_varmap(&varmap, DType::F32, &dev);
    let embedding_model = CategoricalEmbeddings::new(5, 5, vs.clone())?;
    let model = LinearModel::new(input_dim, output_dim, vs.clone())?;
    let mut opt = candle_nn::AdamW::new(
        varmap.all_vars(),
        candle_nn::ParamsAdamW {
            lr: learning_rate,
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
            num_received = receiver.recv_many(&mut items, batch_size) => {
                epoch += 1;
                println!("num received: {num_received}");
                let (data_vec, categorical_vec, labels_vec): (Vec<f32>, Vec<u32>, Vec<f32>) =
                    items
                        .into_iter()
                        .fold((vec![], vec![], vec![]), |(mut td, mut temb, mut tl), item| {
                            // build non-categorical features
                            td.push(item.req.age);
                            td.push(item.req.dist);
                            td.push(item.req.rating);
                            // build categorical features
                            temb.push(item.req.order_type);
                            temb.push(item.req.vehicle_type);
                            // build labels
                            tl.push(item.resp.delivery_time);
                            (td, temb, tl)
                        });
                let num_items = labels_vec.len();
                let features_tensor = Tensor::from_vec(data_vec, (num_items, 3), &dev)?;
                let categories_tensor = Tensor::from_vec(categorical_vec, (num_items, 2), &dev)?;
                let labels_tensor = Tensor::from_vec(labels_vec, num_items, &dev)?;
                let embeddings = embedding_model.forward(&categories_tensor)?;
                let train_data = Tensor::cat(&[&features_tensor, &embeddings], 1)?;
                let preds = model.forward(&train_data)?.squeeze(1)?;
                let epoch_loss = loss::mse(&preds, &labels_tensor)?;
                opt.backward_step(&epoch_loss)?;

                println!("Train Loss: {:8.5}", epoch_loss.to_scalar::<f32>()? / num_items as f32);
                if epoch % 10 == 0 {
                    println!("Saving model after {epoch} epochs");
                    varmap.save(weights_path)?;
                }
            }
        }
        sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings = Settings::new()?;
    let dev = Device::cuda_if_available(0)?;
    let cli = Cli::parse();

    match cli.mode {
        TrainingMode::Streaming => {
            // Setup channels
            let (tx, rx) = channel::<TrainingItem>(settings.train.batch_size);
            let t1 = tokio::spawn(async move {
                training_item_stream(
                    &settings.train.streaming.topic,
                    &settings.train.streaming.brokers,
                    tx,
                )
                .await
            });
            let t2 = tokio::spawn(async move {
                train_from_redpanda(
                    settings.train.input_dim,
                    settings.train.output_dim,
                    settings.train.learning_rate,
                    settings.train.batch_size,
                    &settings.common.weights_path,
                    rx,
                )
                .await
            });
            let (_, _) = (t1.await??, t2.await??);
        }
        TrainingMode::Traditional => match train_from_local(
            settings.train.input_dim,
            settings.train.output_dim,
            settings.train.learning_rate,
            &settings.common.weights_path,
            &dev,
        ) {
            Ok(_) => {}
            Err(e) => {
                println!("Error: {}", e);
                return Err(e);
            }
        },
    };

    Ok(())
}
