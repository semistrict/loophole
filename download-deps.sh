#!/bin/bash

set -eux

DEPS_DIR=third_party

LWEXT4_SHA=58bcf89a121b72d4fb66334f1693d3b30e4cb9c5
CONTAINERS_STORAGE_SHA=83cf57466529353aced8f1803f2302698e0b5cb7
CONTAINER_LIBS_SHA=026c3538f3d1
PODMAN_SHA=1492cea16e08df16417c5256ac82e7bc442a2c2e
NBD_SHA=26fd0eb6c6a519dd9c7123f1c384369ffb02b569
MEROVIUS_NBD_SHA=fd65a54c9949

mkdir -p "$DEPS_DIR"

clone_at() {
  local repo=$1 dir=$2 sha=$3
  if [ ! -d "$dir" ]; then
    git clone --filter=blob:none "$repo" "$dir"
    git -C "$dir" checkout "$sha"
  fi
}

clone_at git@github.com:gkostka/lwext4.git "$DEPS_DIR/lwext4" "$LWEXT4_SHA"
clone_at https://github.com/containers/storage.git "$DEPS_DIR/containers-storage" "$CONTAINERS_STORAGE_SHA"
clone_at https://github.com/containers/container-libs.git "$DEPS_DIR/container-libs" "$CONTAINER_LIBS_SHA"
clone_at https://github.com/containers/podman.git "$DEPS_DIR/podman" "$PODMAN_SHA"
clone_at https://github.com/NetworkBlockDevice/nbd.git "$DEPS_DIR/nbd" "$NBD_SHA"
clone_at https://github.com/Merovius/nbd.git "$DEPS_DIR/merovius-nbd" "$MEROVIUS_NBD_SHA"
