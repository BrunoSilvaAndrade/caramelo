use rdkafka::config::{FromClientConfig, ClientConfig};
use pyo3::{pyclass, pymethods, PyResult};
use rdkafka::client::DefaultClientContext;
use rdkafka::admin::AdminClient;
use rdkafka::message::Headers;
use rdkafka::Message;
use crate::kafka::topic::{Partition, Topic};

#[pyclass]
pub struct Cluster {
    admin: AdminClient<DefaultClientContext>,
    client_config: ClientConfig,
}

#[pymethods]
impl Cluster {
    pub fn __getattr__(&self, name: &str) -> PyResult<Topic> {
        self.get_topic(name)
    }

    pub fn get_topic(&self, name: &str) -> PyResult<Topic> {
        let metadata = self.admin.inner().fetch_metadata(None, None).expect("Failed to fetch metadata");
        let topic = metadata.topics().iter().find(|topic| topic.name() == name)
            .expect(&format!("Topic {} not found", name));

        let partitions: Vec<_> = topic.partitions().iter()
            .map(|p| Partition::new(p.id()))
            .collect();

        Ok(Topic::new(self.client_config.clone(), topic.name().to_string(), partitions.as_slice()))
    }
}

impl Cluster{
    pub fn new(client_config: ClientConfig) -> Self {
        let admin = AdminClient::from_config(&client_config).unwrap();
        Cluster{admin, client_config}
    }
}