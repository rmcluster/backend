#!/bin/sh
OPENAPI_GENERATOR_IMAGE="docker.io/openapitools/openapi-generator-cli"
OPENAPI_GENERATOR_DEST_REL="openapi/generated"

if command -v docker; then
    CONTAINER_ENGINE=docker
elif command -v podman; then
    CONTAINER_ENGINE=podman 
else
    echo "No container engine found. Please install docker or podman."
    exit 1
fi

# generate server stubs
rm -rf "${OPENAPI_GENERATOR_DEST_REL}/go"
mkdir -p "${OPENAPI_GENERATOR_DEST_REL}"
$CONTAINER_ENGINE run --rm -v "${PWD}:/local:ro,z" -v "${PWD}/${OPENAPI_GENERATOR_DEST_REL}:/local/${OPENAPI_GENERATOR_DEST_REL}:z" "${OPENAPI_GENERATOR_IMAGE}" generate \
    -i /local/openapi.yaml \
    -g go-gin-server \
    -o "/local/${OPENAPI_GENERATOR_DEST_REL}" \
    --additional-properties=interfaceOnly=true