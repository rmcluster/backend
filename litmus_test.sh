#!/bin/bash
LITMUS_DOCKER_IMAGE=ghcr.io/rmcluster/litmus-docker:main
RESULTS_DIR="litmus/results"
DATA_TMPDIR="litmus/data"

if command -v docker &> /dev/null; then
    DOCKER=docker
elif command -v podman &> /dev/null; then
    DOCKER=podman
else
    echo "No suitable container runtime installed. Install Docker or Podman." >&2
    exit 1
fi
echo "Using $DOCKER for container runtime." >&2

# make sure the background processes are cleaned up on exit
trap 'kill -9 0' EXIT

# clean up old data
rm -r litmus
mkdir -p "$DATA_TMPDIR"
mkdir -p "$RESULTS_DIR"
OWN_PID=$$

# spin up server
( 
    </dev/null go run . -gcasdb "$DATA_TMPDIR/gcas.db" -storagedb "$DATA_TMPDIR/storage.db" >"$RESULTS_DIR/server.log" 2>&1
    echo "Server stopped."
    kill $OWN_PID
) &

# wait for server to be ready
while ! grep "Listening on http://0.0.0.0:4917" "$RESULTS_DIR/server.log" &> /dev/null; do
    sleep 1
done

# spin up a node
( 
    </dev/null go run ./cmd/linux-client/ -id node-1 -cas-path "$DATA_TMPDIR/cas" -tracker localhost:4917 -port 1921 >"$RESULTS_DIR/node.log" -announce-ip 127.0.0.1 2>&1
    echo "Node stopped."
    kill $OWN_PID
) &

# wait for node to register
while ! grep "New announce from node-1" "$RESULTS_DIR/server.log" &> /dev/null; do
    sleep 1
done

sleep 1

# "$DOCKER" pull "$LITMUS_DOCKER_IMAGE"
"$DOCKER" run -it --rm --net host "$LITMUS_DOCKER_IMAGE" -k dav://localhost:4917/dav 2>&1 | tee "$RESULTS_DIR/litmus.log"