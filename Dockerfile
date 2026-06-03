FROM docker.io/golang:1.26.4@sha256:68cb6d68bed024785b69195b89af7ac7a444f27791435f98647edff595aa0479 AS builder
WORKDIR /build-dir

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o rmd-server .

FROM ghcr.io/rmcluster/llama.cpp-rpc:server@sha256:45ca3a255f43bbead13202635ee97f37e39ac96605672319f57f617f9a6e2264
COPY --from=builder /build-dir/rmd-server /usr/local/bin/rmd-server

# llama.cpp's docker image puts the executables in /app
ENV PATH=/app:$PATH
ENV RMD_METADATA_DB_PATH=/var/lib/rmd/metadata.db
ENV RMD_MODEL_STORAGE_DIR=/var/lib/rmd/models
RUN mkdir -p /var/lib/rmd
RUN mkdir -p /var/lib/rmd/models
VOLUME ["/var/lib/rmd"]
ENTRYPOINT [ "rmd-server", "-host", "0.0.0.0", "-port", "4917" ]
EXPOSE 4917
