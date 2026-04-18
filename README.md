# Reused Mobile Devices

Server and Client for running an automatically managed llama RPC cluster.

## Run Server

```sh
go run .
```

## Web UI

The long-term frontend is now being moved to React/Vite under [frontend](frontend).

The current server-rendered pages remain available for transition and fallback. The main pages are:

- `/` for the landing page and navigation
- `/dashboard` for connected device status
- `/models` for model selection
- `/chat` for the streaming chat interface

For the new frontend, run the React dev server in [frontend](frontend) and point it at the API routes under `/api/ui`.

## Metadata Cache

Hugging Face model metadata is cached in an embedded BoltDB file.

- Local default path: `~/Library/Caches/rmd/metadata.db` (macOS)
- Docker default path: `/var/lib/rmd/metadata.db`
- Override path with `RMD_METADATA_DB_PATH`

For Docker, keep `/var/lib/rmd` on a persistent volume so metadata survives container restarts.

## Local Model Storage

Uploaded local `.gguf` models are stored under:

- Local default path: `~/Library/Caches/rmd/models` (macOS)
- Docker default path: `/var/lib/rmd/models`
- Override path with `RMD_MODEL_STORAGE_DIR`

## Run Client

The client will announce itself to the tracker and run the rpc server.
Replace `/path/to/rpc-server` with the path to the rpc server binary.
Replace `127.0.0.1:4917` with the ip and port of the tracker.

```sh
go run ./cmd/linux-client/ -cmd /path/to/rpc-server -tracker 127.0.0.1:4917 -- -c
```
