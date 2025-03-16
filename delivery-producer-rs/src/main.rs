use std::io::{self, BufRead};
use std::{fs::File, sync::mpsc::channel};

use futures::StreamExt;
use rand::prelude::IndexedRandom;
use samsa::prelude::*;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bootstrap_addrs = vec![
        BrokerAddress {
            host: "127.0.0.1".to_owned(),
            port: 19092,
        },
        BrokerAddress {
            host: "127.0.0.1".to_owned(),
            port: 29092,
        },
        BrokerAddress {
            host: "127.0.0.1".to_owned(),
            port: 39092,
        },
    ];
    let topic_name = "raw-data";

    let (tx, rx) = channel();
    ctrlc::set_handler(move || tx.send(()).expect("Could not send signal on channel."))
        .expect("Error setting Ctrl-C handler");

    let file = File::open("deliverytime.txt")?;
    let mut reader = io::BufReader::new(file);
    let items: Vec<bytes::Bytes> = reader
        .lines()
        .skip(1)
        .map(|item| bytes::Bytes::from(item.expect("unable to string record")))
        .collect();

    let stream = futures::stream::iter(std::iter::repeat(0)).map(move |_| {
        ProduceMessage {
            topic: topic_name.to_string(),
            partition_id: 0,
            key: None,
            //value: Some(bytes::Bytes::from_static(b"unimplemented!est")),
            value: items.choose(&mut rand::rng()).cloned(),
            headers: vec![],
        }
    });

    let output_stream =
        ProducerBuilder::<TcpConnection>::new(bootstrap_addrs, vec![topic_name.to_string()])
            .await
            .expect("connection error")
            .batch_timeout_ms(100)
            .clone()
            .build_from_stream(tokio_stream::StreamExt::chunks_timeout(
                stream,
                20,
                std::time::Duration::from_secs(3),
            ))
            .await;

    tokio::pin!(output_stream);
    while let Some(responses) = output_stream.next().await {
        if rx.try_recv().is_ok() {
            break;
        }
        println!("Messages Sent: {}", responses.len());
        sleep(Duration::from_millis(300)).await;
    }

    Ok(())
}
