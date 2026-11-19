#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
    printf 'Usage: %s BUILD_DIR [PDC CMake options...]\n' "$0" >&2
    exit 2
fi

benchmark=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source_dir=${PDC_SOURCE:-"$benchmark/../.."}
mkdir -p "$1"
build=$(cd -- "$1" && pwd)
shift
mercury=${MERCURY_PREFIX:?Set MERCURY_PREFIX to the installed Mercury prefix}
hdf5=${HDF5_DIR:?Load the HDF5 module or set HDF5_DIR}
compiler=${CC:-cc}
cmake_bin=${CMAKE:-cmake}
mercury_lib=${MERCURY_LIBDIR:-"$mercury/lib"}
hdf5_lib=${HDF5_LIBDIR:-"$hdf5/lib"}
dim1=${DIM1:-10240}
dim2=${DIM2:-10240}

"$cmake_bin" -S "$source_dir" -B "$build/pdc" \
    -DCMAKE_C_COMPILER="$compiler" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX="$build/install" \
    -DBUILD_SHARED_LIBS=ON -DBUILD_TESTING=OFF -DPDC_ENABLE_MPI=ON \
    -DPDC_ENABLE_MULTITHREAD=ON -DPDC_ENABLE_PROFILING=OFF -DPDC_ENABLE_LUSTRE=OFF \
    -DPDC_SERVER_CACHE=ON -DPDC_SERVER_CACHE_MAX_GB=32 -DPDC_SERVER_IDLE_CACHE_FLUSH_TIME=3 \
    -DMERCURY_INCLUDE_DIR="$mercury/include" -DMERCURY_LIBRARY="$mercury_lib/libmercury.so" \
    -DMERCURY_NA_LIBRARY="$mercury_lib/libna.so" -DMERCURY_UTIL_LIBRARY="$mercury_lib/libmercury_util.so" \
    "$@"
"$cmake_bin" --build "$build/pdc" --parallel "${BUILD_JOBS:-2}"
"$cmake_bin" --install "$build/pdc"

mkdir -p "$build/bin"
link_pdc=(-I"$build/install/include" -L"$build/pdc/bin" -lpdc "-Wl,-rpath,$build/pdc/bin")
link_hdf5=(-I"$hdf5/include" -L"$hdf5_lib" -lhdf5 "-Wl,-rpath,$hdf5_lib")
record=(-O2 -g "-DDIM1=$dim1" "-DDIM2=$dim2")
for role in generator reader; do
    "$compiler" "${record[@]}" "$benchmark/pdc_$role.c" "${link_pdc[@]}" -o "$build/bin/pdc_$role"
    "$compiler" "${record[@]}" -DNUM_EPOCHS=1 "$benchmark/hdf5_$role.c" "${link_hdf5[@]}" \
        -o "$build/bin/hdf5_$role"
done
"$compiler" "${record[@]}" "$benchmark/verify_full.c" "${link_pdc[@]}" "${link_hdf5[@]}" \
    -o "$build/bin/verify_full"
for helper in affinity library; do
    "$compiler" -O2 -shared -fPIC "$benchmark/${helper}_report.c" -ldl -o "$build/bin/lib${helper}_report.so"
done
printf '%s\n' "$((dim1 * dim2))" > "$build/record-bytes.txt"
