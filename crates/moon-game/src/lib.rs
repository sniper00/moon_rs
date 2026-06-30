//! `moon-game`: a collection of pure-Rust gameplay algorithms for moon_rs.
//!
//! The crate is intentionally free of any Lua / FFI dependencies so the
//! algorithms can be unit-tested and benchmarked in isolation. Native (Lua)
//! bindings, if needed, live in `moon-runtime` and wrap these types.
//!
//! Modules:
//! - [`math`]: small integer/float geometry primitives shared across algorithms.
//! - [`aoi`]: grid-based Area of Interest system (watcher/marker events).

pub mod aoi;
pub mod math;
