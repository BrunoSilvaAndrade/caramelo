use pyo3::{ffi, Bound, PyErr, PyObject, PyResult, Python};
use pyo3::prelude::PyAnyMethods;
use pyo3::types::PyDict;
use pyo3::ffi::c_str;
use pyo3::intern;

use crate::python::pre_compiler::PreCompiler;
use crate::kafka::cluster::Cluster;
use crate::kafka::rust_object::RustObject;
use crate::python::to_rust_object::ToRustObject;

pub struct Py{
    pre_compiler: PreCompiler,
    to_rust_object: ToRustObject,
}

impl Default for Py{
    fn default() -> Self {
        let pre_compiler = PreCompiler::default();
        let to_rust_object = ToRustObject::default();

        Py{pre_compiler, to_rust_object}
    }
}

impl Py{
    pub fn run(&self, code: &str, cluster: Cluster) -> PyResult<Option<RustObject>> {
        let code_obj = self.pre_compiler.compile(code)?;

        self.run_code_object(&code_obj, cluster)
    }


    /// Runs code object in the given context.
    ///
    /// If `globals` is `None`, it defaults to Python module `__main__`.
    /// If `locals` is `None`, it defaults to the value of `globals`.
    fn run_code_object(&self,
        code_obj: &PyObject,
        cluster: Cluster
    ) -> PyResult<Option<RustObject>> {
        Python::with_gil(|py| {
            let globals = self.default_globals(py)?;
            self.prepare_globals(py, &globals)?;
            globals.set_item("cluster", cluster)?;

            unsafe {
                let result = ffi::PyEval_EvalCode(code_obj.as_ptr(), globals.as_ptr(), globals.as_ptr());
                let _ = Bound::from_borrowed_ptr_or_err(py, result)?;
            }

            if globals.contains("__rust_result")?{
                let result = globals.get_item("__rust_result")?;
                return Ok(Some(self.to_rust_object.to_message_object(py, &result)?));
            }

            Ok(None)
        })

    }

    fn default_globals<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let m_ptr = unsafe {
            let m_ptr = ffi::compat::PyImport_AddModuleRef(c_str!("__main__").as_ptr());
            Bound::from_owned_ptr_or_err(py, m_ptr)?
        };

        let attr = m_ptr.getattr(intern!(py, "__dict__"))?;
        Ok(attr.downcast::<PyDict>()?.to_owned())
    }

    fn prepare_globals(&self, py: Python, globals: &Bound<PyDict>) -> PyResult<()> {
        // If `globals` don't provide `__builtins__`, most of the code will fail if Python
        // version is <3.10. That's probably not what user intended, so insert `__builtins__`
        // for them.
        //
        // See also:
        // - https://github.com/python/cpython/pull/24564 (the same fix in CPython 3.10)
        // - https://github.com/PyO3/pyo3/issues/3370
        let builtins_s = intern!(py, "__builtins__");
        let has_builtins = globals.contains(builtins_s)?;
        if !has_builtins {
            pyo3::sync::with_critical_section(&globals, || {
                // check if another thread set __builtins__ while this thread was blocked on the critical section
                let has_builtins = globals.contains(builtins_s)?;
                if !has_builtins {
                    // Inherit current builtins.
                    let builtins = unsafe { ffi::PyEval_GetBuiltins() };

                    // `PyDict_SetItem` doesn't take ownership of `builtins`, but `PyEval_GetBuiltins`
                    // seems to return a borrowed reference, so no leak here.
                    let c_return = unsafe {
                        ffi::PyDict_SetItem(globals.as_ptr(), builtins_s.as_ptr(), builtins)
                    };

                    if c_return == -1 {
                        return Err(PyErr::fetch(py));
                    }
                }

                Ok(())
            })?;
        };

        Ok(())
    }
}