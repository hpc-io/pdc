#!/bin/bash
# Builds ADIOS2 (with the exact feature set adios2_analysis_test/ needs:
# MPI, Derived Variables, ZFP, Sodium/libsodium; SST+UCX explicitly off,
# see below) from source, installs it, then builds the four benchmark
# programs in this directory against it.
#
# Assumes zfp and libsodium are already built and installed somewhere
# findable by CMake -- set ZFP_PREFIX/SODIUM_PREFIX below if they're not
# at this script's defaults. Needs git, cmake (>=3.12), and an
# MPI-enabled compiler wrapper (mpicc/mpicxx) already on PATH.
#
# Usage: ./build_adios2.sh
# Override any variable by exporting it first, e.g.:
#   ADIOS2_PREFIX=$HOME/adios2-install ./build_adios2.sh
#
# ZFP and Sodium are only needed by adios2_bench_compression and
# adios2_bench_compression_encryption (adios2_bench_magnitude/_curl use
# neither). If your zfp/libsodium install predates or doesn't export a
# modern CMake package-config file (zfp-config.cmake /
# sodium-config.cmake -- ADIOS2 needs the real package-config, not just
# a *_ROOT_DIR-style custom Find module, which some older builds, e.g.
# ones set up for PDC's own ZFP_ROOT_DIR convention, don't provide),
# ADIOS2's own configure will fail outright even though the library
# itself is perfectly usable. Skip the affected feature to unblock the
# other two programs immediately:
#   ENABLE_ZFP=OFF ./build_adios2.sh
#   ENABLE_SODIUM=OFF ./build_adios2.sh
ENABLE_ZFP="${ENABLE_ZFP:-ON}"
ENABLE_SODIUM="${ENABLE_SODIUM:-ON}"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Auto-derive a workspace root by assuming this PDC checkout lives at
# <workspace>/source/pdc (adios2_analysis_test's grandparent's parent),
# and that zfp/libsodium/adios2 install prefixes are siblings under
# <workspace>/install/ -- the convention this repo's own scripts already
# rely on (e.g. vpicio_scale.sh's PDC_DATA_LOC=.../work_space/install/pdc
# next to .../work_space/source/pdc). Only used as a *default*; every
# path below is still directly overridable by exporting the variable
# first, since this guess is wrong for any other layout.
WORKSPACE_ROOT_GUESS="$(cd "$SCRIPT_DIR/../../../.." 2>/dev/null && pwd || true)"

ADIOS2_VERSION="${ADIOS2_VERSION:-v2.12.1}"
ADIOS2_SRC_DIR="${ADIOS2_SRC_DIR:-$SCRIPT_DIR/.adios2_src}"
ADIOS2_BUILD_DIR="${ADIOS2_BUILD_DIR:-$SCRIPT_DIR/.adios2_build}"
ADIOS2_PREFIX="${ADIOS2_PREFIX:-$WORKSPACE_ROOT_GUESS/install/adios2}"
ZFP_PREFIX="${ZFP_PREFIX:-$WORKSPACE_ROOT_GUESS/install/zfp}"
SODIUM_PREFIX="${SODIUM_PREFIX:-$WORKSPACE_ROOT_GUESS/install/libsodium}"
JOBS="${JOBS:-$(nproc)}"

echo "== ADIOS2 version:    $ADIOS2_VERSION"
echo "== ADIOS2 source:     $ADIOS2_SRC_DIR"
echo "== ADIOS2 build dir:  $ADIOS2_BUILD_DIR"
echo "== ADIOS2 install to: $ADIOS2_PREFIX"
echo "== zfp prefix:        $ZFP_PREFIX"
echo "== libsodium prefix:  $SODIUM_PREFIX"
echo "== parallel jobs:     $JOBS"
echo

if [ "$ENABLE_ZFP" = "ON" ] && [ ! -d "$ZFP_PREFIX" ]; then
  echo "ERROR: ZFP_PREFIX ($ZFP_PREFIX) does not exist." >&2
  echo "  This path was guessed as \$WORKSPACE_ROOT/install/zfp -- if zfp is built" >&2
  echo "  and installed somewhere else on this machine, rerun as:" >&2
  echo "    ZFP_PREFIX=/path/to/zfp/install $0" >&2
  echo "  If zfp isn't built at all yet, either build/install it first, or skip" >&2
  echo "  it with ENABLE_ZFP=OFF (see this script's header comment)." >&2
  exit 1
fi
if [ "$ENABLE_SODIUM" = "ON" ] && [ ! -d "$SODIUM_PREFIX" ]; then
  echo "ERROR: SODIUM_PREFIX ($SODIUM_PREFIX) does not exist." >&2
  echo "  This path was guessed as \$WORKSPACE_ROOT/install/libsodium -- if libsodium" >&2
  echo "  is built and installed somewhere else on this machine, rerun as:" >&2
  echo "    SODIUM_PREFIX=/path/to/libsodium/install $0" >&2
  echo "  If libsodium isn't built at all yet, either build/install it first, or" >&2
  echo "  skip it with ENABLE_SODIUM=OFF (see this script's header comment)." >&2
  exit 1
fi

if [ ! -d "$ADIOS2_SRC_DIR" ]; then
  echo "== Cloning ADIOS2 $ADIOS2_VERSION..."
  git clone --quiet https://github.com/ornladios/ADIOS2.git "$ADIOS2_SRC_DIR"
fi
git -C "$ADIOS2_SRC_DIR" checkout --quiet --force "$ADIOS2_VERSION"

# v2.12.1's CURL derived-variable operator (used by adios2_bench_curl_inflight)
# has a real indexing bug in ApplyCurl: it differences along the wrong axis
# relative to the row-major (last-dim-fastest) stride convention the rest of
# ADIOS2 assumes, producing wrong (not just zero) output -- confirmed by hand
# and via bpls against a known analytic curl. This patch replaces ApplyCurl's
# body with a direct port of src/tests/analysis/curl_math.h's own convention
# (same stencil, same one-sided boundary handling), verified to reproduce the
# exact expected result at 1 and 4 ranks. See
# patches/0001-fix-curl-derived-variable-indexing.patch's diff for the
# before/after. `git apply --check` first so this is a no-op (not an error)
# if the patch is already applied, e.g. on a rerun against an already-patched
# checkout.
PATCH="$SCRIPT_DIR/patches/0001-fix-curl-derived-variable-indexing.patch"
if git -C "$ADIOS2_SRC_DIR" apply --check "$PATCH" 2>/dev/null; then
  echo "== Applying CURL derived-variable fix..."
  git -C "$ADIOS2_SRC_DIR" apply "$PATCH"
elif git -C "$ADIOS2_SRC_DIR" apply --reverse --check "$PATCH" 2>/dev/null; then
  echo "== CURL derived-variable fix already applied, skipping."
else
  echo "ERROR: $PATCH does not apply cleanly to $ADIOS2_SRC_DIR at $ADIOS2_VERSION." >&2
  echo "  ADIOS2's source may have changed around this code since the patch was written." >&2
  exit 1
fi

# Always a fresh configure: ADIOS2's generated ADIOSConfig.h has been
# seen not to pick up a feature-flag change on an incremental
# `cmake .` reconfigure of an existing build dir (Derived_Variable
# specifically), so this directory is wiped every run rather than risk
# a stale, half-updated build.
echo "== Configuring (clean)..."
rm -rf "$ADIOS2_BUILD_DIR"
mkdir -p "$ADIOS2_BUILD_DIR"

CMAKE_PREFIX_PATH_PARTS=()
[ "$ENABLE_ZFP" = "ON" ] && CMAKE_PREFIX_PATH_PARTS+=("$ZFP_PREFIX")
[ "$ENABLE_SODIUM" = "ON" ] && CMAKE_PREFIX_PATH_PARTS+=("$SODIUM_PREFIX")
CMAKE_PREFIX_PATH_JOINED="$(IFS=';'; echo "${CMAKE_PREFIX_PATH_PARTS[*]:-}")"

cmake -S "$ADIOS2_SRC_DIR" -B "$ADIOS2_BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$ADIOS2_PREFIX" \
  -DCMAKE_C_COMPILER=mpicc \
  -DCMAKE_CXX_COMPILER=mpicxx \
  -DCMAKE_PREFIX_PATH="$CMAKE_PREFIX_PATH_JOINED" \
  -DADIOS2_USE_MPI=ON \
  -DADIOS2_USE_HDF5=OFF \
  -DADIOS2_USE_CUDA=OFF \
  -DADIOS2_USE_Kokkos=OFF \
  -DADIOS2_USE_Fortran=OFF \
  -DADIOS2_USE_Python=OFF \
  -DADIOS2_USE_ZeroMQ=OFF \
  -DADIOS2_USE_SST=OFF \
  -DADIOS2_USE_UCX=OFF \
  -DADIOS2_USE_Derived_Variable=ON \
  -DADIOS2_USE_ZFP="$ENABLE_ZFP" \
  -DADIOS2_USE_Sodium="$ENABLE_SODIUM" \
  -DBUILD_TESTING=OFF \
  -DADIOS2_BUILD_EXAMPLES=OFF

echo "== Building ($JOBS jobs)..."
cmake --build "$ADIOS2_BUILD_DIR" -j"$JOBS"

echo "== Installing to $ADIOS2_PREFIX..."
cmake --install "$ADIOS2_BUILD_DIR"

# The four ADIOS2 benchmark directories (moved here when
# pdc_helper_scripts/ was reorganized by analysis/transformation/io --
# this script used to build in-place in a single adios2_analysis_test/
# directory that no longer exists).
BENCH_DIRS=(
  "$SCRIPT_DIR/../analysis/magnitude/adios2"
  "$SCRIPT_DIR/../analysis/curl/adios2"
  "$SCRIPT_DIR/../transformation/adios2_compression"
  "$SCRIPT_DIR/../io/adios2_vpicio"
)
echo "== Building ADIOS2 benchmark programs..."
for d in "${BENCH_DIRS[@]}"; do
  echo "-- $d"
  ADIOS2_PREFIX="$ADIOS2_PREFIX" make -C "$d" clean
  ADIOS2_PREFIX="$ADIOS2_PREFIX" make -C "$d"
done

echo
echo "== Done. Binaries:"
for d in "${BENCH_DIRS[@]}"; do
  ls -1 "$d"/adios2_bench_* "$d"/adios2_vpicio* "$d"/adios2_bdcats* 2>/dev/null
done
if [ "$ENABLE_ZFP" != "ON" ] || [ "$ENABLE_SODIUM" != "ON" ]; then
  echo
  echo "NOTE: built with ENABLE_ZFP=$ENABLE_ZFP ENABLE_SODIUM=$ENABLE_SODIUM."
  echo "  adios2_bench_magnitude, adios2_bench_curl, and adios2_bench_curl_inflight"
  echo "  are unaffected, but adios2_bench_compression /"
  echo "  adios2_bench_compression_encryption will fail at runtime (not compile"
  echo "  time) with an unrecognized-operator error if the corresponding feature"
  echo "  is off."
fi
