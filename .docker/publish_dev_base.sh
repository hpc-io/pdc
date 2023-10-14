#!/bin/bash

# Check if $1 is unset or empty
if [ "$#" -lt 2 ]; then
    echo "./publish_dev_base.sh <docker_registry_namespace> <version> <process_manifest:0/1>"
    echo "For building and publishing image, just ignore the second argument. For processing manifest, set the second argument to 1."
    echo "For best performance, you need to run this script on the same architecture as the target image. But you may specify the second argument to 1 on the machine where fastest network is available to process the manifest."
    exit 1
fi

IMG_NS=$1
VERSION=$2
arch=$(uname -m)
ARCH_CODE=""

case $arch in
    x86_64)
        ARCH_CODE="amd64"
        echo "You are running on x86_64 (AMD64) architecture."
        ;;
    arm64 | aarch64)
        ARCH_CODE="arm64v8"
        echo "You are running on ARM64 (AArch64) architecture."
        ;;
    i386 | i686)
        ARCH_CODE="i386"
        echo "You are running on x86 (32-bit) architecture."
        ;;
    arm*)
        ARCH_CODE="arm32v7"
        echo "You are running on ARM (32-bit) architecture."
        ;;
    ppc64le)
        ARCH_CODE="ppc64le"
        echo "You are running on PowerPC (64-bit little-endian) architecture."
        ;;
    s390x)
        ARCH_CODE="s390x"
        echo "You are running on IBM Z (s390x) architecture."
        ;;
    *)
        echo "Unknown or unsupported architecture: $arch"
        exit 1
        ;;
esac

if [ -z "$3" ] || [ "$3" -eq 0 ]; then
    docker build -t ${IMG_NS}/pdc_dev_base:${VERSION}-${ARCH_CODE} -f .docker/dev_base.Dockerfile --build-arg ARCH=${ARCH_CODE}/ .
    docker push ${IMG_NS}/pdc_dev_base:${VERSION}-${ARCH_CODE}
    exit 0
else
    echo "Processing manifest..."
    # Process manifest
    # arch_strings=("amd64" "arm64v8" "i386" "arm32v7" "ppc64le" "s390x")
    arch_strings=("amd64" "arm64v8")
    manifest_args=()
    for arch in "${arch_strings[@]}"; do
        echo "Processing architecture: $arch"
        manifest_args+=("--amend" "${IMG_NS}/pdc_dev_base:${VERSION}-${arch}")
        if [[  "$arch" == "$ARCH_CODE" ]]; then
            echo "Skipping pulling current architecture: $arch"
            continue
        fi
        docker pull ${IMG_NS}/pdc_dev_base:${VERSION}-${arch}
    done

    docker manifest create ${IMG_NS}/pdc_dev_base:${VERSION} ${manifest_args[@]}
    docker manifest push ${IMG_NS}/pdc_dev_base:${VERSION}
    docker manifest rm ${IMG_NS}/pdc_dev_base:latest
    docker manifest create ${IMG_NS}/pdc_dev_base:latest ${manifest_args[@]}
    docker manifest push ${IMG_NS}/pdc_dev_base:latest

fi


