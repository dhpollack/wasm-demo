use std::str::FromStr;
use std::{borrow::Borrow, error::Error};

use redpanda_transform_sdk::*;
use serde::{Deserialize, Serialize};
use serde_repr::Serialize_repr;

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

#[derive(Clone, Serialize, Deserialize)]
struct TrainingRecord {
    age: u32,
    rating: f32,
    dist: f32,
    order_type: OrderType,
    vehicle_type: VehicleType,
    delivery_time: f32,
}

#[derive(Serialize_repr, Deserialize, Clone, Debug)]
#[repr(u8)]
enum OrderType {
    Unknown = 0,
    Buffet = 1,
    Drinks = 2,
    Meal = 3,
    Snack = 4,
}

impl FromStr for OrderType {
    type Err = ();
    fn from_str(input: &str) -> Result<OrderType, Self::Err> {
        match input.trim().to_lowercase().as_str() {
            "buffet" => Ok(OrderType::Buffet),
            "drinks" => Ok(OrderType::Drinks),
            "meal" => Ok(OrderType::Meal),
            "snack" => Ok(OrderType::Snack),
            _ => Ok(OrderType::Unknown),
        }
    }
}

#[derive(Serialize_repr, Deserialize, Clone, Debug)]
#[repr(u8)]
enum VehicleType {
    Unknown = 0,
    Bicycle = 1,
    ElectricScooter = 2,
    Motorcycle = 3,
    Scooter = 4,
}

impl FromStr for VehicleType {
    type Err = ();
    fn from_str(input: &str) -> Result<VehicleType, Self::Err> {
        match input.trim().to_lowercase().as_str() {
            "bicycle" => Ok(VehicleType::Bicycle),
            "electric_scooter" => Ok(VehicleType::ElectricScooter),
            "motorcycle" => Ok(VehicleType::Motorcycle),
            "scooter" => Ok(VehicleType::Scooter),
            _ => Ok(VehicleType::Unknown),
        }
    }
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
    let rec: RawRecord = match event.record.value() {
        Some(val) if !val.is_empty() => {
            serde_json::from_slice(val).expect("unable to deserialize stream data from redpanda")
        }
        _ => return Ok(()),
    };

    let dist = calc_dist(rec.r_lat, rec.r_lon, rec.d_lat, rec.d_lon);
    // TODO: do this automatically in serialization
    let order_type = OrderType::from_str(rec.order_type.as_str()).expect("order type found");
    let vehicle_type = VehicleType::from_str(rec.vehicle_type.as_str()).expect("order type found");

    let training_rec = TrainingRecord {
        age: rec.age,
        rating: rec.rating,
        dist,
        order_type,
        vehicle_type,
        delivery_time: rec.delivery_time,
    };
    let out = serde_json::to_string(&training_rec)?;
    let res = out.into_bytes();

    let new_rec = Record::new(Some(rec.pid.into_bytes()), Some(res));

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization() {
        let data = r#"{"age":37,"d_lat":22.765049,"d_lon":75.912471,"delivery_time":24,"id":"4607","order_type":"snack","pid":"INDORES13DEL02","r_lat":22.745049,"r_lon":75.892471,"rating":4.9,"vehicle_type":"motorcycle"}"#;
        let rec: RawRecord = serde_json::from_str(data).expect("raw record from json string");
        let order_type = OrderType::from_str(rec.order_type.as_str()).expect("unable to convert");
        let vehicle_type =
            VehicleType::from_str(rec.vehicle_type.as_str()).expect("unable to convert");
        let training_rec = TrainingRecord {
            age: rec.age,
            rating: rec.rating,
            dist: 10.0,
            order_type,
            vehicle_type,
            delivery_time: rec.delivery_time,
        };
        let training_rec_json = serde_json::to_string(&training_rec)
            .expect("unable to serialize TrainingRecord to json");
        println!("{:?}", training_rec_json);
    }
}
