#[macro_use]
extern crate rocket;

use std::sync::Mutex;

use candle_core::{DType, Device, Tensor};
use candle_nn::{VarBuilder, VarMap};
use rocket::{serde::json::Json, State};
use serde::Serialize;
use utoipa::{OpenApi, ToSchema};
use utoipa_redoc::{Redoc, Servable};

use delivery_model::data::{InferenceRequest, InferenceResponse};
use delivery_model::model::{CategoricalEmbeddings, LinearModel};
use delivery_model::settings::Settings;

#[derive(OpenApi)]
#[openapi(paths(index, reload))]
struct InferenceApi;

#[derive(Serialize, ToSchema, Responder, Debug)]
enum InferenceError {
    #[response(status = 500)]
    ServerError(String),
}

struct ModelState {
    model: Mutex<LinearModel>,
    embedding_model: Mutex<CategoricalEmbeddings>,
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
    rocket::debug!("{:?}", item);
    let features = Tensor::new(vec![vec![item.age, item.dist, item.rating]], device)
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?;
    let categories = Tensor::new(vec![vec![item.order_type, item.vehicle_type]], device)
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?;
    let m = model_state.model.lock().unwrap();
    let emb = model_state.embedding_model.lock().unwrap();
    let embeddings = emb
        .forward(&categories)
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?;
    let input = Tensor::cat(&[&features, &embeddings], 1)
        .map_err(|err| InferenceError::ServerError(format!("{err}")))?;
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
fn reload(model_state: &State<ModelState>, dev: &State<Device>, settings: &State<Settings>) {
    let weights = candle_core::safetensors::load(settings.common.weights_path.as_str(), dev)
        .expect("could not load tensors");
    let mut m = model_state.model.lock().unwrap();
    let mut emb = model_state.embedding_model.lock().unwrap();
    m.reload(weights.clone());
    emb.reload(weights);
}

fn load_model(
    settings: &Settings,
    dev: &Device,
) -> anyhow::Result<(LinearModel, CategoricalEmbeddings)> {
    let (model, embedding_model) =
        match candle_core::safetensors::load(&settings.common.weights_path, dev) {
            Ok(weights) => (
                LinearModel::load(weights.clone())?,
                CategoricalEmbeddings::load(weights)?,
            ),
            Err(_) => {
                let varmap = VarMap::new();
                let vs = VarBuilder::from_varmap(&varmap, DType::F32, &dev);
                let model = LinearModel::new(
                    settings.train.input_dim,
                    settings.train.output_dim,
                    vs.clone(),
                )?;
                let embedding_model = CategoricalEmbeddings::new(
                    settings.train.num_order_types,
                    settings.train.num_vehicle_types,
                    vs.clone(),
                )?;
                (model, embedding_model)
            }
        };
    Ok((model, embedding_model))
}

#[rocket::main]
async fn main() -> anyhow::Result<()> {
    // Get settings
    let settings = Settings::new()?;
    // dump openapi file
    let openapi = InferenceApi::openapi();
    let _ = std::fs::write("./openapi.json", openapi.to_pretty_json().unwrap());

    // setup model
    let dev = Device::cuda_if_available(0)?;
    let (model, embedding_model) = load_model(&settings, &dev)?;
    let model_state = ModelState {
        model: Mutex::new(model),
        embedding_model: Mutex::new(embedding_model),
    };

    rocket::build()
        .manage(model_state)
        .manage(dev)
        .manage(settings)
        .mount("/", routes![index, reload])
        .mount("/", Redoc::with_url("/redoc", openapi))
        .launch()
        .await?;
    Ok(())
}
