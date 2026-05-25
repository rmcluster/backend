# Reused Mobile Devices

Server and Client for running an automatically managed llama RPC cluster.

## Run Server

```sh
go run .
```

## Web UI

The long-term frontend is now being moved to React/Vite under [rmcluster/frontend](https://github.com/rmcluster/frontend).

The current server-rendered pages remain available for transition and fallback. The currently available pages are:

- `/` for the landing page and navigation
- `/dashboard` for connected device status

For the new frontend, run the React dev server from [rmcluster/frontend](https://github.com/rmcluster/frontend) and point it at the API routes under `/api/ui`.

## Metadata Cache

Hugging Face model metadata is cached in an embedded BoltDB file.

- Default path: `$XDG_CACHE_HOME/rmd/metadata.db` (Linux), `~/Library/Caches/rmd/metadata.db` (macOS)
- Docker default path: `/var/cache/rmd/metadata.db`
- Override path with `RMD_METADATA_DB_PATH`

For Docker, mount `/var/cache/rmd` as a persistent volume so metadata survives container restarts.

## Local Model Storage

Uploaded local `.gguf` models are stored under:

- Default path: `$XDG_DATA_HOME/rmd/models` (Linux), `~/.local/share/rmd/models` (fallback)
- Override path with `RMD_MODEL_STORAGE_DIR`

## Run Client

The client will announce itself to the tracker and run the rpc server.
Replace `/path/to/rpc-server` with the path to the rpc server binary.
Replace `127.0.0.1:4917` with the ip and port of the tracker.

```sh
go run ./cmd/linux-client/ -cmd /path/to/rpc-server -tracker 127.0.0.1:4917 -- -c
```

## Storage Benchmarks

The storage benchmark runner automates two experiments:

- `devices vs upload time` using each phone's native `/chunk` API
- `chunk size vs download time` using the server `/dav` WebDAV layer and `/api/ui/storage-chunk-size`

Prerequisites:

- the rmcluster server is already running
- one or more storage-capable devices are connected and announcing to the tracker
- Python has `requests` and `matplotlib` available

Example:

```sh
python3 scripts/storage_bench.py \
  --server-url http://127.0.0.1:4917 \
  --file-size-mib 10 \
  --repetitions 3
```

Artifacts are written to a timestamped directory under `server/benchmarks/results/`, for example:

- `devices_vs_upload_time.png`
- `chunk_size_vs_download_time.png`
- `devices_vs_upload_time.csv`
- `chunk_size_vs_download_time.csv`
- `run_metadata.json`
