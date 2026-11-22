FROM ubuntu:24.04
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        cloud-image-utils ovmf qemu-system-x86 qemu-utils \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /vm
