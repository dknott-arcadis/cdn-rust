build: target/debug/cdn-rust

target/release/cdn-rust: src/main.rs Cargo.toml
	cargo build --release

target/debug/cdn-rust: src/main.rs Cargo.toml
	cargo build

run: target/debug/cdn-rust
	cargo run

release: target/release/cdn-rust
