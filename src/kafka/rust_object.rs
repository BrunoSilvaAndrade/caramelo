use std::collections::HashMap;
use pyo3::{pyclass, pymethods};
use crate::kafka::rust_object::ObjectType::{BOOL, BYTES, FLOAT, INT, LIST, OBJECT, STR};

#[derive(Clone, Debug)]
pub enum ObjectType {
    OBJECT,
    LIST,
    STR,
    FLOAT,
    INT,
    BOOL,
    BYTES
}

#[pyclass]
#[derive(Clone, Debug)]
pub struct RustObject {
    _type: ObjectType,
    list: Option<Vec<RustObject>>,
    attrs: Option<HashMap<String, Box<RustObject>>>,
    str: Option<String>,
    float: Option<f64>,
    int: Option<i64>,
    bool: Option<bool>,
    bytes: Option<Vec<u8>>,
}

#[pymethods]
impl RustObject {
    #[staticmethod]
    pub fn list() -> Self{
        RustObject {_type: LIST, list: Some(Vec::new()), attrs: None, str: None, float: None, int: None, bool: None, bytes: None}
    }

    #[staticmethod]
    pub fn obj() -> Self{
        RustObject {_type: OBJECT, list: None, attrs: Some(HashMap::new()), str: None, float: None, int: None, bool: None, bytes: None}
    }

    #[staticmethod]
    pub fn str(str: String) -> Self{
        RustObject {_type: STR, list: None, attrs: None, str: Some(str), float: None, int: None, bool: None, bytes: None}
    }

    #[staticmethod]
    pub fn float(float: f64) -> Self{
        RustObject {_type: FLOAT, list: None, attrs: None, str: None, float: Some(float), int: None, bool: None, bytes: None}
    }

    #[staticmethod]
    pub fn int(int: i64) -> Self{
        RustObject {_type: INT, list: None, attrs: None, str: None, float: None, int: Some(int), bool: None, bytes: None}
    }

    #[staticmethod]
    pub fn bool(b: bool) -> Self{
        RustObject {_type: BOOL, list: None, attrs: None, str: None, float: None, int: None, bool: Some(b), bytes: None}
    }

    #[staticmethod]
    pub fn bytes(bytes: Vec<u8>) -> Self{
        RustObject {_type: BYTES, list: None, attrs: None, str: None, float: None, int: None, bool: None, bytes: Some(bytes)}
    }

    pub fn append(&mut self, message_obj: RustObject){
        self.list.as_mut().unwrap().push(message_obj);
    }

    pub fn set_attr(&mut self, key: String, value: RustObject) {
        self.attrs.as_mut().unwrap().insert(key, Box::new(value));
    }

    pub fn remove_attr(&mut self, key: String) {
        self.attrs.as_mut().unwrap().remove(&key);
    }
}