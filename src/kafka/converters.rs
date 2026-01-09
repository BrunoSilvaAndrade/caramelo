use pyo3::{IntoPyObjectExt, Py, PyAny, Python};

pub type Converter = fn(Option<&[u8]>) -> Py<PyAny>;

pub struct Converters;

impl Converters {
    pub fn u8_to_hex(bytes: Option<&[u8]>) -> Py<PyAny> {
        Python::with_gil(|py| -> Py<PyAny> {
            if let Some(bytes) = bytes {
                return hex::encode(bytes).into_py_any(py).unwrap();
            }

            py.None()
        })
    }

    pub fn u8_to_str(bytes: Option<&[u8]>) -> Py<PyAny> {
        Python::with_gil(|py| -> Py<PyAny> {
            if let Some(bytes) = bytes {
                return String::from_utf8_lossy(bytes).into_py_any(py).unwrap();
            }

            py.None()
        })
    }
}