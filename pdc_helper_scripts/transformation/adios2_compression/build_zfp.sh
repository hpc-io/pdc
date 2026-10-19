#!/bin/bash
# Builds zfp from source and installs it with a real CMake package-config
# export (zfp-config.cmake), which is what ADIOS2's find_package(ZFP)
# requires (CONFIG mode, not the older *_ROOT_DIR-style Find-module
# convention PDC's own build uses -- see build_adios2.sh's header
# comment). Installs to a SEPARATE prefix by default, deliberately not
# reusing/overwriting any existing zfp install PDC itself is already
# linked against on this machine.
#
# Usage: ./build_zfp.sh
# Override any variable by exporting it first, e.g.:
#   ZFP_PREFIX=$HOME/zfp-adios2-install ./build_zfp.sh
#
# GPU (CUDA) zfp support is OFF by default -- adios2_bench_compression /
# adios2_bench_compression_encryption only ever run zfp on the CPU. If
# you want GPU zfp too:
#   ENABLE_CUDA=ON ./build_zfp.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Same auto-derivation convention as build_adios2.sh: assumes this PDC
# checkout lives at <workspace>/source/pdc, and install prefixes are
# siblings under <workspace>/install/.
WORKSPACE_ROOT_GUESS="$(cd "$SCRIPT_DIR/../../../../.." 2>/dev/null && pwd || true)"

ZFP_VERSION="${ZFP_VERSION:-1.0.1}"
ZFP_SRC_DIR="${ZFP_SRC_DIR:-$SCRIPT_DIR/.zfp_src}"
ZFP_BUILD_DIR="${ZFP_BUILD_DIR:-$SCRIPT_DIR/.zfp_build}"
# Deliberately a different name than the "plain" workspace zfp install
# (install/zfp) so this never collides with / overwrites whatever PDC
# itself is already built against.
ZFP_PREFIX="${ZFP_PREFIX:-$WORKSPACE_ROOT_GUESS/install/zfp_adios2}"
ENABLE_CUDA="${ENABLE_CUDA:-OFF}"
CUDA_ARCH="${CUDA_ARCH:-native}"
JOBS="${JOBS:-$(nproc)}"

echo "== zfp version:     $ZFP_VERSION"
echo "== zfp source:      $ZFP_SRC_DIR"
echo "== zfp build dir:   $ZFP_BUILD_DIR"
echo "== zfp install to:  $ZFP_PREFIX"
echo "== CUDA support:    $ENABLE_CUDA"
echo "== parallel jobs:   $JOBS"
echo

if [ ! -d "$ZFP_SRC_DIR" ]; then
  echo "== Cloning zfp $ZFP_VERSION..."
  git clone --quiet https://github.com/LLNL/zfp.git "$ZFP_SRC_DIR"
fi
git -C "$ZFP_SRC_DIR" checkout --quiet "$ZFP_VERSION"

echo "== Configuring (clean)..."
rm -rf "$ZFP_BUILD_DIR"
mkdir -p "$ZFP_BUILD_DIR"

CMAKE_ARGS=(
  -S "$ZFP_SRC_DIR" -B "$ZFP_BUILD_DIR"
  -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_INSTALL_PREFIX="$ZFP_PREFIX"
  -DBUILD_SHARED_LIBS=ON
  -DBUILD_TESTING=OFF
  -DBUILD_EXAMPLES=OFF
  -DBUILD_UTILITIES=OFF
)
if [ "$ENABLE_CUDA" = "ON" ]; then
  CMAKE_ARGS+=(-DZFP_WITH_CUDA=ON -DCMAKE_CUDA_ARCHITECTURES="$CUDA_ARCH")
fi

cmake "${CMAKE_ARGS[@]}"

echo "== Building ($JOBS jobs)..."
cmake --build "$ZFP_BUILD_DIR" -j"$JOBS"

echo "== Installing to $ZFP_PREFIX..."
cmake --install "$ZFP_BUILD_DIR"

echo
echo "== Verifying CMake package-config export..."
FOUND_CONFIG="$(find "$ZFP_PREFIX" -iname "zfp-config.cmake" 2>/dev/null | head -n1 || true)"
if [ -n "$FOUND_CONFIG" ]; then
  echo "OK: found $FOUND_CONFIG"
  echo
  echo "Build ADIOS2 against this zfp with:"
  echo "  ZFP_PREFIX=$ZFP_PREFIX ./build_adios2.sh"
else
  echo "WARNING: no zfp-config.cmake found anywhere under $ZFP_PREFIX." >&2
  echo "  ADIOS2's find_package(ZFP) will still fail against this install." >&2
  find "$ZFP_PREFIX" -iname "*.cmake" >&2 || true
  exit 1
fi
