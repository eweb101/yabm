FROM rust:1.45.2 as builder
WORKDIR /usr/src

RUN USER=root cargo new yabm
WORKDIR /usr/src/yabm
COPY Cargo.toml Cargo.lock ./
RUN set -x\
 && mkdir -p src\
 && echo "fn main() {println!(\"broken\")}" > src/main.rs\
 && cargo build --release

COPY src ./src
RUN set -x\
 && touch src/*.rs\
 && cargo install --locked --path .

FROM ubuntu
RUN apt-get update \
    && apt-get install -y mysql-client python3-pip\
    && rm -rf /var/lib/apt/lists/*\
    && pip3 install awscli
COPY --from=builder /usr/local/cargo/bin/yabm /usr/local/bin/yabm
CMD ["yabm"]
