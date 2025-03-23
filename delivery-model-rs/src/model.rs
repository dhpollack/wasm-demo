use std::collections::HashMap;

use candle_core::{IndexOp, Result, Tensor};
use candle_nn::{embedding, linear, Embedding, Linear, Module, VarBuilder};

pub struct LinearModel {
    first: Linear,
    second: Linear,
}

impl LinearModel {
    pub fn new(input_dim: usize, output_dim: usize, vs: VarBuilder) -> anyhow::Result<Self> {
        let first = linear(input_dim, 100, vs.pp("ln1"))?;
        let second = linear(100, output_dim, vs.pp("ln2"))?;
        Ok(Self { first, second })
    }
    pub fn forward(&self, x: &Tensor) -> Result<Tensor> {
        let x = self.first.forward(x)?;
        let x = x.gelu()?;
        self.second.forward(&x)
    }
    pub fn load(weights: HashMap<String, Tensor>) -> Result<Self> {
        let first = Linear::new(
            weights.get("ln1.weight").unwrap().clone(),
            Some(weights.get("ln1.bias").unwrap().clone()),
        );
        let second = Linear::new(
            weights.get("ln2.weight").unwrap().clone(),
            Some(weights.get("ln2.bias").unwrap().clone()),
        );
        Ok(Self { first, second })
    }
    pub fn reload(&mut self, weights: HashMap<String, Tensor>) {
        let first = Linear::new(
            weights.get("ln1.weight").unwrap().clone(),
            Some(weights.get("ln1.bias").unwrap().clone()),
        );
        let second = Linear::new(
            weights.get("ln2.weight").unwrap().clone(),
            Some(weights.get("ln2.bias").unwrap().clone()),
        );
        self.first.clone_from(&first);
        self.second.clone_from(&second);
    }
}

pub struct CategoricalEmbeddings {
    order_type_emb: Embedding,
    vehicle_type_emb: Embedding,
}

impl CategoricalEmbeddings {
    pub fn new(
        num_order_types: usize,
        num_vehicle_types: usize,
        vs: VarBuilder,
    ) -> anyhow::Result<Self> {
        let order_type_emb = embedding(num_order_types, 10, vs.pp("order_type_emb"))?;
        let vehicle_type_emb = embedding(num_vehicle_types, 10, vs.pp("vehicle_type_emb"))?;
        Ok(Self {
            order_type_emb,
            vehicle_type_emb,
        })
    }
    pub fn forward(&self, x: &Tensor) -> Result<Tensor> {
        let order_input = x.i((.., 0))?.contiguous()?;
        let vehicle_input = x.i((.., 1))?.contiguous()?;
        let oemb = self.order_type_emb.forward(&order_input)?;
        let vemb = self.vehicle_type_emb.forward(&vehicle_input)?;
        Tensor::cat(&[&oemb, &vemb], 1)
    }
    pub fn load(weights: HashMap<String, Tensor>) -> Result<Self> {
        let order_type_emb = Embedding::new(
            weights
                .get("order_type_emb.weight")
                .expect("could not find order type embeddings")
                .clone(),
            weights.get("order_type_emb.weight").unwrap().dim(1)?,
        );
        let vehicle_type_emb = Embedding::new(
            weights
                .get("vehicle_type_emb.weight")
                .expect("could not find vehicle type embeddings")
                .clone(),
            weights.get("vehicle_type_emb.weight").unwrap().dim(1)?,
        );
        Ok(Self {
            order_type_emb,
            vehicle_type_emb,
        })
    }
    pub fn reload(&mut self, weights: HashMap<String, Tensor>) {
        let order_type_emb = Embedding::new(
            weights
                .get("order_type_emb.weight")
                .expect("could not find order type embeddings")
                .clone(),
            weights
                .get("order_type_emb.weight")
                .unwrap()
                .dim(1)
                .expect("issue with order type dimensions"),
        );
        let vehicle_type_emb = Embedding::new(
            weights
                .get("vehicle_type_emb.weight")
                .expect("could not find vehicle type embeddings")
                .clone(),
            weights
                .get("vehicle_type_emb.weight")
                .unwrap()
                .dim(1)
                .expect("issue with vehicle type dimensions"),
        );
        self.order_type_emb.clone_from(&order_type_emb);
        self.vehicle_type_emb.clone_from(&vehicle_type_emb);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_core::{DType, Device, Shape};
    use candle_nn::VarMap;

    #[test]
    fn test_categorical_embeddings() {
        let dev = Device::cuda_if_available(0).expect("cannot create device");
        let varmap = VarMap::new();
        let vs = VarBuilder::from_varmap(&varmap, DType::F32, &dev);

        let embedding_model =
            CategoricalEmbeddings::new(4, 5, vs.clone()).expect("cannot create model");

        let input = Tensor::new(
            vec![vec![2_u32, 4], vec![1, 1], vec![2, 1], vec![3, 1]],
            &dev,
        )
        .expect("unable to create input tensor");
        let output = embedding_model
            .forward(&input)
            .expect("unable to run forward pass");
        let output_shape = output.shape();
        let expected_shape = Shape::from_dims(&[4, 20]);
        assert_eq!(output_shape.to_owned(), expected_shape);
    }

    #[test]
    fn test_linear_model() {
        let dev = Device::cuda_if_available(0).expect("cannot create device");
        let varmap = VarMap::new();
        let vs = VarBuilder::from_varmap(&varmap, DType::F32, &dev);

        let model = LinearModel::new(3, 1, vs.clone()).expect("unable to create model");

        let input = Tensor::new(vec![vec![1.0_f32, 0.5, 0.6], vec![2.0, 2.5, 0.1]], &dev)
            .expect("unable to create input");
        let input_shape = input.shape();
        let expected_input_shape = Shape::from_dims(&[2, 3]);
        assert_eq!(input_shape.to_owned(), expected_input_shape);
        let output = model.forward(&input).expect("unable to run forward pass");
        let output_shape = output.shape();
        let expected_shape = Shape::from_dims(&[2, 1]);
        assert_eq!(output_shape.to_owned(), expected_shape);
    }
}
