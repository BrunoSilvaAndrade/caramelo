use std::ffi::CStr;
use pyo3::ffi::c_str;
use pyo3::prelude::PyModule;
use pyo3::{Py, PyObject, PyResult, Python};

const PY_PRE_COMPILER: &CStr = c_str!(include_str!("pre_compiler/py_compiler.py"));

pub(crate) struct PreCompiler {
    py_compiler_mod: Py<PyModule>,
}

impl Default for PreCompiler {
    fn default() -> Self {
        let module = Python::with_gil(|py| {
            return PyModule::from_code(py, PY_PRE_COMPILER, c_str!("<string>"), c_str!("py_compiler"))
                .expect("Unable to load py_compiler module")
                .unbind();
        });

        PreCompiler {py_compiler_mod: module}
    }
}

impl PreCompiler {
    pub(crate) fn compile(&self, code: &str) -> PyResult<PyObject>{
        Python::with_gil(|py| {
            let compile_function =  self.py_compiler_mod.getattr(py,"compile_str_code")?;

            return Ok(compile_function.call1(py, (code,))?);
        })
    }
}