use std::fs::File;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{self, BufRead};
use std::sync::mpsc::channel;
use std::time::Duration;

use log::debug;
use rand::prelude::IndexedRandom;

use config::Config;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time::{sleep, Duration as TokioDuration};

#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
struct AppConfig {
    topic: String,
    brokers: String,
    batch_size: i32,
    loop_sleep: u64,
    data_file: String,
}

fn load_data(data_file: &str) -> anyhow::Result<Vec<String>> {
    println!("Load data...");
    let file = File::open(data_file)?;
    let reader = io::BufReader::new(file);
    let items: Vec<String> = reader
        .lines()
        .map(|item| item.expect("unable to string record"))
        .collect();
    Ok(items)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config: AppConfig = Config::builder()
        .add_source(config::File::with_name("config/default"))
        .add_source(config::Environment::with_prefix("PRODUCER"))
        .build()?
        .try_deserialize()?;

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).expect("Could not send signal on channel."))
        .expect("Error setting Ctrl-C handler");

    let topic = config.topic.as_str();
    let batch_size = config.batch_size;
    let loop_sleep = config.loop_sleep;

    println!("Create producer...");
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    let items = load_data(config.data_file.as_str())?;
    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    println!("Begin loop...");
    loop {
        if rx.try_recv().is_ok() {
            break;
        }
        let futures = (0..batch_size)
            .map(|_| items.choose(&mut rand::rng()).cloned().unwrap())
            .map(|item| async move {
                // The send operation on the topic returns a future, which will be
                // completed once the result or failure from Kafka is received.
                let mut hasher = DefaultHasher::new();
                item.hash(&mut hasher);
                let key = format!("{:x}", hasher.finish());

                let delivery_status = producer
                    .send(
                        FutureRecord::to(topic)
                            .payload(&item)
                            .key(&key)
                            .headers(OwnedHeaders::new()),
                        Duration::from_secs(0),
                    )
                    .await;

                // This will be executed when the result is received.
                debug!("Delivery status for message received");
                delivery_status
            })
            .collect::<Vec<_>>();
        let mut last_res = Ok((0, 0));
        // This loop will wait until all delivery statuses have been received.
        for fut in futures {
            last_res = fut.await;
        }
        println!("Next loop iteraton: {:?}", last_res);
        sleep(TokioDuration::from_millis(loop_sleep)).await;
    }
    println!("Fin!");
    Ok(())
}
