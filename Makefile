build: target/debug/cdn-rust

target/release/cdn-rust: src/main.rs
	cargo build --release

target/debug/cdn-rust: src/main.rs
	cargo build

run: target/debug/cdn-rust
	cargo run

release: target/release/cdn-rust
