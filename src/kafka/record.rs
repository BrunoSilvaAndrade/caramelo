use pyo3::{pyclass, pymethods, Py, PyAny, PyRefMut};

#[pyclass]
pub struct Record{
    topic: String,
    headers: Vec<Header>,
    key: Py<PyAny>,
    value: Py<PyAny>,
}

#[pymethods]
impl Record {
    #[new]
    pub fn new(topic: &str, headers: Vec<Header>, key: Py<PyAny>, value: Py<PyAny>) -> Self {
        Record{topic: topic.to_owned(), headers, key, value}
    }

    pub fn key(&self) -> String {
        self.key.to_string()
    }

    pub fn value(slf: PyRefMut<Self>) -> Py<PyAny> {
        slf.value.clone_ref(slf.py())
    }

    pub fn headers(&self) -> Vec<Header> {
        let mut headers = Vec::new();
        headers.clone_from(&self.headers);

        headers
    }
}

#[pyclass]
#[derive(Clone)]
pub struct Header{
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[pymethods]
impl Header{
    #[new]
    pub fn new(key: &str, value: Option<Vec<u8>>) -> Self{
        let key = key.to_owned();
        Header{key, value}
    }
}