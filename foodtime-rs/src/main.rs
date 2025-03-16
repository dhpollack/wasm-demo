use std::{borrow::Borrow, error::Error};

use redpanda_transform_sdk::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct RawRecord {
    id: String,
    pid: String,
    age: u32,
    rating: f32,
    r_lat: f32,
    r_lon: f32,
    d_lat: f32,
    d_lon: f32,
    order_type: String,
    vehicle_type: String,
    delivery_time: f32,
}

#[derive(Serialize, Deserialize)]
struct TrainingRecord {
    age: u32,
    rating: f32,
    dist: f32,
}

const R: f32 = 6371.0;

fn main() {
    // Register your transform function.
    // This is a good place to perform other setup too.
    on_record_written(transform);
}

// my_transform is where you read the record that was written, and then you can
// return new records that will be written to the output topic
fn transform(event: WriteEvent, writer: &mut RecordWriter) -> Result<(), Box<dyn Error>> {
    let data = match event.record.value() {
        Some(val) if !val.is_empty() => val,
        _ => return Ok(()),
    };

    let rec = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(data)
        .deserialize::<RawRecord>()
        .next()
        .expect("no item found")?;

    let dist = calc_dist(rec.r_lat, rec.r_lon, rec.d_lat, rec.d_lon);

    let training_rec = TrainingRecord {
        age: rec.age,
        rating: rec.rating,
        dist,
    };
    let mut out = csv::Writer::from_writer(vec![]);
    out.serialize(training_rec)
        .expect("unable to serialize TrainingRecord");
    let res = out.into_inner()?;

    let new_rec = Record::new(Some(rec.id.into_bytes()), Some(res));

    writer
        .write(new_rec.borrow())
        .expect("unable to write training record");
    Ok(())
}

fn deg_to_rad(degrees: f32) -> f32 {
    degrees * (std::f32::consts::PI / 180.0)
}

fn calc_dist(lat1: f32, lon1: f32, lat2: f32, lon2: f32) -> f32 {
    let d_lat = deg_to_rad(lat2 - lat1);
    let d_lon = deg_to_rad(lon2 - lon1);
    let a = (d_lat / 2.0).sin().powi(2)
        + (deg_to_rad(lat1)).cos() * (deg_to_rad(lat2)).cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    R * c
}
