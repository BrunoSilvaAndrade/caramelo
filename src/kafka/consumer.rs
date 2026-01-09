use std::ops::Deref;
use std::time::Duration;
use pyo3::{pyclass, pymethods, Bound, Py, PyErr, PyRefMut};
use pyo3::types::{PyFunction};
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer as RDKafkaConsumer};
use rdkafka::{Message, TopicPartitionList};
use rdkafka::error::KafkaResult;
use rdkafka::message::Headers;
use crate::kafka::converters::{Converter, Converters};
use crate::kafka::record::{Header, Record};
use crate::kafka::topic::Topic;

#[pyclass]
pub struct Consumer{
    consumer: BaseConsumer,
    value_converter: Converter,
    key_converter: Converter,
    filter: Option<Py<PyFunction>>,
    is_count: bool,
    limit: usize,
}

impl Consumer{
    pub fn new(topic: &Topic, filter: Option<Py<PyFunction>>, is_count: bool) -> KafkaResult<Self> {
        let consumer = BaseConsumer::from_config(&topic.client_config())?;
        let mut plist = TopicPartitionList::with_capacity(topic.partitions().len());
        topic.partitions().iter().for_each(|p|{
            plist.add_partition(topic.name(), p.id());
        });

        consumer.assign(&plist)?;

        let value_converter = Converters::u8_to_hex;
        let key_converter = Converters::u8_to_hex;

        Ok(Self{consumer, value_converter, key_converter, filter, is_count, limit: 50})
    }
}


#[pymethods]
impl Consumer {
    pub fn value_as_hex<'p>(mut slf: PyRefMut<'p, Self>)-> PyRefMut<'p, Self>{
        slf.value_converter = Converters::u8_to_hex;
        slf
    }

    pub fn key_as_hex<'p>(mut slf: PyRefMut<'p, Self>)-> PyRefMut<'p, Self>{
        slf.key_converter = Converters::u8_to_hex;
        slf
    }

    pub fn value_as_str<'p>(mut slf: PyRefMut<'p, Self>)-> PyRefMut<'p, Self>{
        slf.value_converter = Converters::u8_to_str;
        slf
    }

    pub fn key_as_str<'p>(mut slf: PyRefMut<'p, Self>)-> PyRefMut<'p, Self>{
        slf.key_converter = Converters::u8_to_str;
        slf
    }

    pub fn limit<'p>(mut slf: PyRefMut<'p, Self>, limit: usize) -> PyRefMut<'p, Consumer> {
        slf.limit = limit;
        slf
    }

    pub fn find<'p>(mut slf: PyRefMut<'p, Self>, filter: Option<Bound<PyFunction>>) -> PyRefMut<'p, Consumer> {
        slf.filter(filter);
        slf
    }

    pub fn count<'p>(mut slf: PyRefMut<'p, Self>, filter: Option<Bound<PyFunction>>) -> PyRefMut<'p, Consumer>{
        slf.filter(filter);
        slf.is_count = true;
        slf
    }

    pub fn filter(&mut self, filter: Option<Bound<PyFunction>>){
        if self.filter.is_some() {
            self.filter = Some(filter.unwrap().unbind())
        }
    }

    pub fn __iter__(&self) -> ConsumerIter{
        ConsumerIter{records: self.execute()}
    }

    pub fn execute(&self) -> Vec<Record> {
        let mut records = Vec::with_capacity(self.limit);
        let value_converter = self.value_converter;
        let key_converter = self.key_converter;
        let poll_timeout_ms = 20;
        let timeout_ms = 1500;
        let mut time_spent = 0;

        loop{
            let option = self.consumer.poll(Duration::from_millis(poll_timeout_ms));

            if let Some(result) = option {
                let m = result
                    .map_err(|e| {
                        PyErr::new::<Consumer, String>(e.to_string())
                    }).unwrap();

                let headers = m.headers().map(|headers| {
                    headers.iter().map(|header|{
                        let value = header.value.map(|v| v.to_owned());
                        Header::new(header.key, value)
                    }).collect()
                }).unwrap_or(Vec::new());

                let value = value_converter(m.key());
                let key = key_converter(m.payload());

                let record = Record::new(m.topic(), headers, key, value);
                records.push(record);
                if records.len() >= self.limit{
                    return records;
                }
            }else{
                time_spent += poll_timeout_ms;
            }

            if time_spent >= timeout_ms {
                return records;
            }
        }
    }
}

#[pyclass]
pub struct ConsumerIter{
    records: Vec<Record>,
}

#[pymethods]
impl ConsumerIter{
    pub fn __iter__(slf: PyRefMut<Self>) -> PyRefMut<Self>{
        slf
    }

    pub fn __next__(&mut self) -> Option<Record>{
        self.records.pop()
    }
}