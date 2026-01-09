use rdkafka::consumer::Consumer;
mod kafka;
mod python;

use rdkafka::config::{ClientConfig, FromClientConfig};
use rdkafka::{Message};

use pyo3::types::IntoPyDict;
use pyo3::prelude::*;

use python::runner::Py;
use std::any::Any;
use rdkafka::consumer::{Consumer as RDConsumer};
use crate::kafka::cluster::Cluster;

const SCRIPT: &str = include_str!("test.py");

fn main() -> Result<(), PyErr> {
    let client_config = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "smallest")
        .set("group.id", "not_used")
        .to_owned();

    let cluster = Cluster::new(client_config);
    let py = Py::default();

    let obj = py.run(SCRIPT, cluster)?;

    match obj {
        Some(data) => println!("got data {:?}", data),
        _ => {}
    }

    Ok(())
}

