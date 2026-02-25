#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPS_DIR="$SCRIPT_DIR/third_party"
PATCHES_DIR="$SCRIPT_DIR/patches"

LWEXT4_REPO=https://github.com/gkostka/lwext4.git
LWEXT4_SHA=58bcf89a121b72d4fb66334f1693d3b30e4cb9c5

CONTAINERS_STORAGE_REPO=https://github.com/containers/storage.git
CONTAINERS_STORAGE_SHA=83cf57466529353aced8f1803f2302698e0b5cb7

CONTAINER_LIBS_REPO=https://github.com/containers/container-libs.git
CONTAINER_LIBS_SHA=026c3538f3d1

PODMAN_REPO=https://github.com/containers/podman.git
PODMAN_SHA=1492cea16e08df16417c5256ac82e7bc442a2c2e

NBD_REPO=https://github.com/NetworkBlockDevice/nbd.git
NBD_SHA=26fd0eb6c6a519dd9c7123f1c384369ffb02b569

MEROVIUS_NBD_REPO=https://github.com/Merovius/nbd.git
MEROVIUS_NBD_SHA=fd65a54c9949

clone_at() {
  local repo=$1 dir=$2 sha=$3
  if [ -d "$dir" ]; then
    echo "Skipping $dir (already exists)"
    return
  fi
  echo "Cloning $repo at $sha into $dir"
  git clone --filter=blob:none "$repo" "$dir"
  git -C "$dir" checkout "$sha"
}

cmd_download() {
  mkdir -p "$DEPS_DIR"
  clone_at "$LWEXT4_REPO"            "$DEPS_DIR/lwext4"              "$LWEXT4_SHA"
  clone_at "$CONTAINERS_STORAGE_REPO" "$DEPS_DIR/containers-storage" "$CONTAINERS_STORAGE_SHA"
  clone_at "$CONTAINER_LIBS_REPO"     "$DEPS_DIR/container-libs"     "$CONTAINER_LIBS_SHA"
  clone_at "$PODMAN_REPO"             "$DEPS_DIR/podman"             "$PODMAN_SHA"
  clone_at "$NBD_REPO"                "$DEPS_DIR/nbd"                "$NBD_SHA"
  clone_at "$MEROVIUS_NBD_REPO"       "$DEPS_DIR/merovius-nbd"       "$MEROVIUS_NBD_SHA"
}

apply_patch() {
  local dep=$1
  local patch="$PATCHES_DIR/$dep.patch"
  if [ ! -f "$patch" ]; then
    return
  fi
  local dir="$DEPS_DIR/$dep"
  local marker="$dir/.patched"
  if [ -f "$marker" ]; then
    echo "Skipping patch for $dep (already applied)"
    return
  fi
  echo "Applying patch for $dep"
  if is_git_dep "$dep"; then
    git -C "$dir" apply < "$patch"
  else
    patch -d "$dir" -p2 < "$patch"
  fi
  touch "$marker"
}

cmd_patch() {
  apply_patch lwext4
  apply_patch containers-storage
  apply_patch container-libs
  apply_patch merovius-nbd
  apply_patch podman

}

cmd_setup() {
  cmd_download
  cmd_patch
}

PATCHED_DEPS=(lwext4 containers-storage container-libs merovius-nbd podman)

# Deps that use git diff/apply instead of diff -ruN/patch.
GIT_DEPS=(lwext4)

is_git_dep() {
  local dep=$1
  for d in "${GIT_DEPS[@]}"; do
    if [ "$d" = "$dep" ]; then return 0; fi
  done
  return 1
}

cmd_reset() {
  for dep in "${PATCHED_DEPS[@]}"; do
    local dir="$DEPS_DIR/$dep"
    if [ ! -d "$dir/.git" ]; then
      continue
    fi
    echo "Resetting $dep"
    git -C "$dir" checkout .
    git -C "$dir" clean -fd
    rm -f "$dir/.patched"
  done
}

cmd_repatch() {
  cmd_reset
  cmd_patch
}

cmd_clean() {
  echo "Removing $DEPS_DIR"
  rm -rf "$DEPS_DIR"
}

# Map dep names to repo and SHA for genpatch.
dep_repo() {
  case "$1" in
    lwext4)             echo "$LWEXT4_REPO" "$LWEXT4_SHA" ;;
    containers-storage) echo "$CONTAINERS_STORAGE_REPO" "$CONTAINERS_STORAGE_SHA" ;;
    container-libs)     echo "$CONTAINER_LIBS_REPO" "$CONTAINER_LIBS_SHA" ;;
    merovius-nbd)       echo "$MEROVIUS_NBD_REPO" "$MEROVIUS_NBD_SHA" ;;
    podman)             echo "$PODMAN_REPO" "$PODMAN_SHA" ;;
    *) echo "Unknown dep: $1" >&2; return 1 ;;
  esac
}

# Generate a patch using git diff (respects .gitignore).
gen_one_patch_git() {
  local dep=$1
  local working="$DEPS_DIR/$dep"
  local out="$PATCHES_DIR/$dep.patch"

  git -C "$working" diff HEAD > "$out"
  # Append untracked files (that aren't gitignored) as new file diffs.
  local untracked
  untracked=$(git -C "$working" ls-files --others --exclude-standard)
  if [ -n "$untracked" ]; then
    while IFS= read -r f; do
      git -C "$working" diff --no-index /dev/null "$f" >> "$out" || true
    done <<< "$untracked"
  fi
}

# Generate a patch by cloning upstream and diffing against working tree.
gen_one_patch_diff() {
  local dep=$1
  local working="$DEPS_DIR/$dep"
  local out="$PATCHES_DIR/$dep.patch"

  read -r repo sha < <(dep_repo "$dep")
  local tmpdir
  tmpdir=$(mktemp -d)
  trap "rm -rf '$tmpdir'" RETURN

  echo "Cloning upstream $dep at $sha..." >&2
  mkdir -p "$tmpdir/a"
  git clone --filter=blob:none -q "$repo" "$tmpdir/a/$dep"
  git -C "$tmpdir/a/$dep" checkout -q "$sha"

  mkdir -p "$tmpdir/b"
  rsync -a --exclude=.git --exclude=.patched "$working/" "$tmpdir/b/$dep/"

  (cd "$tmpdir" && diff -ruN --exclude=.git --exclude=.patched "a/$dep" "b/$dep") > "$out" || true
}

gen_one_patch() {
  local dep=$1
  local working="$DEPS_DIR/$dep"
  if [ ! -d "$working" ]; then
    echo "Skipping $dep (not downloaded)" >&2
    return
  fi

  mkdir -p "$PATCHES_DIR"
  local out="$PATCHES_DIR/$dep.patch"

  if is_git_dep "$dep"; then
    gen_one_patch_git "$dep"
  else
    gen_one_patch_diff "$dep"
  fi

  if [ -s "$out" ]; then
    echo "Wrote $out"
  else
    rm -f "$out"
    echo "No diff for $dep, removed patch file"
  fi
}

cmd_genpatch() {
  if [ $# -gt 0 ]; then
    for dep in "$@"; do
      gen_one_patch "$dep"
    done
  else
    for dep in "${PATCHED_DEPS[@]}"; do
      gen_one_patch "$dep"
    done
  fi
}

usage() {
  echo "Usage: $0 <command>"
  echo ""
  echo "Commands:"
  echo "  setup      Download dependencies and apply patches (default)"
  echo "  download   Download dependencies only"
  echo "  patch      Apply patches to already-downloaded dependencies"
  echo "  reset      Reset patched deps to upstream state"
  echo "  repatch    Reset and re-apply patches"
  echo "  genpatch   Regenerate patch files from working tree (optionally: genpatch <dep>)"
  echo "  clean      Remove third_party directory"
}

case "${1:-setup}" in
  setup)    cmd_setup ;;
  download) cmd_download ;;
  patch)    cmd_patch ;;
  reset)    cmd_reset ;;
  repatch)  cmd_repatch ;;
  genpatch) shift; cmd_genpatch "$@" ;;
  clean)    cmd_clean ;;
  -h|--help|help) usage ;;
  *)
    echo "Unknown command: $1" >&2
    usage >&2
    exit 1
    ;;
esac
