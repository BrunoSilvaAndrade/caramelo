use pyo3::{pyclass, pymethods, PyErr, PyResult, PyTypeInfo};
use rdkafka::ClientConfig;
use rdkafka::error::KafkaError;
use crate::kafka::consumer::Consumer;

#[pyclass]
#[derive(Clone)]
pub struct Partition {
    id: i32
}

impl Partition {
    pub fn new(id: i32) -> Self {
        Self{id}
    }

    pub fn id(&self) -> i32 {
        self.id
    }
}

#[pyclass]
pub struct Topic {
    name: String,
    client_config: ClientConfig,
    partitions: Vec<Partition>,
}

impl Topic {
    pub fn new(client_config: ClientConfig, name: String, partitions: &[Partition]) -> Self {
        Self {client_config, name, partitions: Vec::from(partitions)}
    }
}

impl Topic{
    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    pub fn name(&self) -> &str{
        &self.name
    }

    pub fn partitions(&self) -> &[Partition] {
        &self.partitions.as_slice()
    }
}

#[pymethods]
impl Topic {
    pub fn find(&self) -> PyResult<Consumer>{
        Consumer::new(&self, None, false)
        .map_err(|e| {
            PyErr::new::<Topic, String>(e.to_string())
        })
    }

    pub fn count(&self) -> PyResult<Consumer>{
        Consumer::new(&self, None, true)
        .map_err(|e| {
            PyErr::new::<Topic, String>(e.to_string())
        })
    }
}
