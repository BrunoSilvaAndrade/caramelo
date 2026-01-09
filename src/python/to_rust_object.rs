use std::ffi::CStr;
use pyo3::ffi::c_str;
use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Bound, Py, PyAny, PyResult, Python};
use crate::kafka::cluster::{Cluster};
use crate::kafka::consumer::Consumer;
use crate::kafka::record::{Header, Record};
use crate::kafka::rust_object::RustObject;
use crate::kafka::topic::Topic;

const TO_MESSAGE_OBJECT: &CStr = c_str!(include_str!("to_rust_object/to_rust_object.py"));

pub(crate) struct ToRustObject {
    to_message_mod: Py<PyModule>,
}

impl Default for ToRustObject {
    fn default() -> Self {
        let module = Python::with_gil(|py| {
            let module = PyModule::from_code(py, TO_MESSAGE_OBJECT, c_str!("<string>"), c_str!("to_message_object"))
                .expect("Unable to load to_message_object module");

            module.add_class::<RustObject>().expect("Unable to add MessageObject class to to_message_object module");
            module.add_class::<Cluster>().expect("Unable to add Cluster class to to_message_object module");
            module.add_class::<Topic>().expect("Unable to add Topic class to to_message_object module");
            module.add_class::<Consumer>().expect("Unable to add Consumer class to to_message_object module");
            module.add_class::<Header>().expect("Unable to add header to to_message_object module");
            module.add_class::<Record>().expect("Unable to add record to to_message_object module");

            module.unbind()
        });

        ToRustObject {to_message_mod: module}
    }
}

impl ToRustObject {
    pub(crate) fn to_message_object(&self, py: Python, py_object: &Bound<PyAny>) -> PyResult<RustObject>{
        let to_message_object_function = self.to_message_mod.getattr(py,"to_rust_object")?;
        let result = to_message_object_function.call1(py, (py_object,));
        result?.extract(py)
    }
}