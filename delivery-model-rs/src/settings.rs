use config::{Config, ConfigError, Environment};
use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct CommonSettings {
    pub weights_path: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct TrainSettings {
    pub input_dim: usize,
    pub output_dim: usize,
    pub num_order_types: usize,
    pub num_vehicle_types: usize,
    pub learning_rate: f64,
    pub batch_size: usize,
    pub streaming: StreamingTrainSettings,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct StreamingTrainSettings {
    pub topic: String,
    pub brokers: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub common: CommonSettings,
    pub train: TrainSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(config::File::with_name("config/default"))
            .add_source(config::File::with_name("config/local").required(false))
            .add_source(Environment::with_prefix("delivery_model"))
            .build()?
            .try_deserialize()
    }
}
