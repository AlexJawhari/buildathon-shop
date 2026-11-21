# Static glibc binary in an empty image: no interpreter, nothing to start but the pipeline.
FROM rust:1.100-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-feature=+crt-static -C target-cpu=x86-64-v3"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/shop /shop

FROM scratch
COPY --from=build /shop /shop
USER 1000:1000
ENTRYPOINT ["/shop"]
