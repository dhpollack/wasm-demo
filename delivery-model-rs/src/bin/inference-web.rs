#[macro_use]
extern crate rocket;

use std::sync::Mutex;

use candle_core::{Device, Tensor};
use rocket::{serde::json::Json, State};
use serde::Serialize;
use utoipa::{OpenApi, ToSchema};
use utoipa_redoc::{Redoc, Servable};

use delivery_model::data::{InferenceRequest, InferenceResponse};
use delivery_model::model::LinearModel;

#[derive(OpenApi)]
#[openapi(paths(index))]
struct InferenceApi;

#[derive(Serialize, ToSchema, Responder, Debug)]
enum InferenceError {
    #[response(status = 500)]
    ServerError(String),
}

struct ModelState {
    model: Mutex<LinearModel>,
}

#[utoipa::path(
    responses(
        (status = 201, description = "Inference response with estimated delivery time", body = InferenceResponse)
    )
)]
#[post("/", data = "<item>")]
fn index(
    item: Json<InferenceRequest>,
    model_state: &State<ModelState>,
    device: &State<Device>,
) -> Result<Json<InferenceResponse>, InferenceError> {
    let input = Tensor::from_vec(vec![item.age, item.dist, item.rating], (1, 3), &device)
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?;
    let m = model_state.model.lock().unwrap();
    let res = m
        .forward(&input)
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?;
    let delivery_time = res
        .squeeze(0)
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?
        .squeeze(0)
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?
        .to_scalar()
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?;
    Ok(Json(InferenceResponse { delivery_time }))
}

#[utoipa::path(
    responses((status = 200, description = "Reload model from safetensors file"))
)]
#[get("/reload")]
fn reload(model_state: &State<ModelState>, dev: &State<Device>) {
    let weights =
        candle_core::safetensors::load("model.safetensors", &dev).expect("could not load tensors");
    let mut m = model_state.model.lock().unwrap();
    m.reload(weights);
}

fn load_model(dev: &Device) -> anyhow::Result<LinearModel> {
    let weights = candle_core::safetensors::load("model.safetensors", &dev)?;
    let model = LinearModel::load(weights)?;
    Ok(model)
}

#[rocket::main]
async fn main() -> anyhow::Result<()> {
    let dev = Device::cuda_if_available(0)?;
    let model_state = ModelState {
        model: Mutex::new(load_model(&dev)?),
    };

    rocket::build()
        .manage(model_state)
        .manage(dev)
        .mount("/", routes![index, reload])
        .mount("/", Redoc::with_url("/redoc", InferenceApi::openapi()))
        .launch()
        .await?;
    Ok(())
}
