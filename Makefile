include local.mk

build: target/debug/cdn-rust

target/release/cdn-rust: src/main.rs Cargo.toml
	cargo build --release

target/debug/cdn-rust: src/main.rs Cargo.toml
	cargo build

run: target/debug/cdn-rust
	cargo run

release: target/release/cdn-rust

run-release: target/release/cdn-rust
	./target/release/cdn-rust

REGISTRY ?= crcitdevdev001.azurecr.io
IMAGE = $(REGISTRY)/platform/cdn-rust
TAG ?= latest

docker-build:
	docker build \
		--pull \
		--progress plain \
		-t $(IMAGE):$(TAG) .

docker-push: docker-build
	docker push $(IMAGE):$(TAG)

docker-run: docker-build
	docker run --rm -it -p 3000:3000 $(IMAGE):$(TAG)
