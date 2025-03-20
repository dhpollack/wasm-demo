use std::fs::File;
use std::io::{self, BufRead};
use std::sync::mpsc::channel;
use std::time::Duration;

use log::debug;
use rand::prelude::IndexedRandom;

use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio::time::{sleep, Duration as TokioDuration};

const BATCH_SIZE: i32 = 20;

fn load_data() -> anyhow::Result<Vec<String>> {
    println!("Load data...");
    let file = File::open("deliverytime.txt")?;
    let reader = io::BufReader::new(file);
    let items: Vec<String> = reader
        .lines()
        .skip(1)
        .map(|item| item.expect("unable to string record"))
        .collect();
    Ok(items)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).expect("Could not send signal on channel."))
        .expect("Error setting Ctrl-C handler");

    let topic = "raw-data";
    let brokers = "0.0.0.0:19092,0.0.0.0:29092,0.0.0.0:39092";

    println!("Create producer...");
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");
    let items = load_data()?;
    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    println!("Begin loop...");
    loop {
        if rx.try_recv().is_ok() {
            break;
        }
        let futures = (0..BATCH_SIZE)
            .map(|_| items.choose(&mut rand::rng()).cloned().unwrap())
            .map(|item| async move {
                // The send operation on the topic returns a future, which will be
                // completed once the result or failure from Kafka is received.
                let key = item.split(',').next().unwrap();

                let delivery_status = producer
                    .send(
                        FutureRecord::to(topic)
                            .payload(&item)
                            .key(key)
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
        sleep(TokioDuration::from_millis(200)).await;
    }
    println!("Fin!");
    Ok(())
}
