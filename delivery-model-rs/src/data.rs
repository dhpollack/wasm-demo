use serde::{Deserialize, Serialize};

use candle_core::Tensor;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, ToSchema)]
pub struct InferenceRequest {
    pub age: f32,
    pub dist: f32,
    pub rating: f32,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct InferenceResponse {
    pub delivery_time: f32,
}

#[derive(Serialize, Deserialize)]
pub struct TrainingItem {
    #[serde(flatten)]
    pub req: InferenceRequest,
    #[serde(flatten)]
    pub resp: InferenceResponse,
}

#[derive(Clone)]
pub struct Dataset {
    pub train_data: Tensor,
    pub train_labels: Tensor,
}
