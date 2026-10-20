FROM rust:1.88.0-bookworm AS build
WORKDIR /work/deckr-core-rust
COPY --from=core /Cargo.toml /Cargo.lock ./
COPY --from=core /src ./src
WORKDIR /work/deckr-integration-tests/agents/rust
COPY agents/rust/Cargo.toml agents/rust/Cargo.lock ./
COPY agents/rust/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/work/deckr-integration-tests/agents/rust/target cargo build --locked --release && cp target/release/deckr-integration-worker /usr/local/bin/worker
FROM debian:bookworm-slim
COPY --from=build /usr/local/bin/worker /usr/local/bin/worker
COPY protocol /protocol
ENV PROTOCOL_DIR=/protocol
ENTRYPOINT ["/usr/local/bin/worker"]
