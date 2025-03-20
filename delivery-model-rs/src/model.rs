use std::collections::HashMap;

use candle_core::{Result, Tensor};
use candle_nn::{linear, Linear, Module, VarBuilder};

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
        let x = x.relu()?;
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
