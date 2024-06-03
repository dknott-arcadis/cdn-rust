FROM rust:1-slim-bookworm as build

# ZScaler Config (Local only)
USER root
COPY zscaler.crt /usr/local/share/ca-certificates/zscaler.crt
RUN cat /usr/local/share/ca-certificates/zscaler.crt >> /etc/ssl/certs/ca-certificates.crt
# End ZScaler Config

RUN apt-get update && apt-get install -y \
    libssl-dev \
    pkg-config \
    mold

RUN cargo new --bin cdn-rust
WORKDIR /cdn-rust

COPY ./Cargo.* /.cargo/ ./

# Cache dependencies layer
RUN cargo build --release
RUN rm src/*.rs

COPY ./src ./src

RUN rm ./target/release/deps/cdn_rust*; rm ./target/release/cdn-rust*; cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /cdn-rust/target/release/cdn-rust .

EXPOSE 3000

CMD ["./cdn-rust"]
