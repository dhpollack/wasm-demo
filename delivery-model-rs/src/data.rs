use serde::{Deserialize, Serialize};

use candle_core::Tensor;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct InferenceRequest {
    pub age: f32,
    pub dist: f32,
    pub rating: f32,
    pub order_type: u32,
    pub vehicle_type: u32,
}

#[derive(Serialize, Deserialize, ToSchema, Debug)]
pub struct InferenceResponse {
    pub delivery_time: f32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TrainingItem {
    #[serde(flatten)]
    pub req: InferenceRequest,
    #[serde(flatten)]
    pub resp: InferenceResponse,
}

#[derive(Clone)]
pub struct TrainData {
    pub features: Tensor,
    pub categories: Tensor,
}

#[derive(Clone)]
pub struct Dataset {
    pub train_data: TrainData,
    pub train_labels: Tensor,
}
