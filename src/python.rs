use pyo3::prelude::*;

#[pymodule]
mod _presolv {
    use pyo3::prelude::*;

    #[pyfunction]
    fn native_version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
