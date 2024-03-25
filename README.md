# Moon in Rust

This project is a reimplementation of `moon` in Rust. It's currently under development.

`moon`, originally implemented in C++&Lua, is a lightweight, high-performance game server framework. This Rust version aims to maintain the simplicity and efficiency of the original, while leveraging the safety and concurrency features of Rust.

Please note that this project is still in the early stages of development. Contributions and feedback are welcome.

## Test

```
cargo run --release assets/example.lua
cargo run --release assets/benchmark_send.lua
```

## Development Status

The project is currently in the development phase. Many features from the original Moon are yet to be implemented.

## Why Rust?

Rust is a modern system programming language focused on performance, reliability, and productivity. It offers the low-level control of C and C++ but with the added benefit of a strong compile-time correctness guarantee. Its rich type system and ownership model guarantee memory safety and thread safety.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.
