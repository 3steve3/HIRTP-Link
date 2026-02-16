FROM rust:1.93-slim as builder
RUN apt-get update && apt-get install -y protobuf-compiler libssl-dev pkg-config curl
WORKDIR /app

COPY . .

WORKDIR /app/Client

RUN cargo build --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app

COPY --from=builder /app/Client/target/release/HIRTP-Link-RemoteClient .

RUN echo '#!/bin/bash\n\
mkdir -p ./pending\n\
if [ ! -f ./pending/jades4.tif ]; then\n\
  echo "Downloading mission data (120MB JADES4 TIFF)..."\n\
  curl -L -o ./pending/jades4.tif https://esawebb.org/media/archives/images/original/jades4.tif\n\
fi\n\
./HIRTP-Link-RemoteClient' > entrypoint.sh && chmod +x entrypoint.sh

CMD ["./entrypoint.sh"]