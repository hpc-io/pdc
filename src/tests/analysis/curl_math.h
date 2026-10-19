/**
 * Shared curl-of-a-vector-field kernel, used both by the server-side
 * external AN function (an_client/curl_kernel.c, eager mode) and the
 * client-side posthoc analyze phase (bench_curl_analyze.c), so the two
 * benchmark modes compute the identical result and stay comparable.
 *
 * Domain layout: a local (nx, ny, nz) block, x fastest-varying
 * (flat index = (k*ny + j)*nx + i), matching how bench_curl_*.c lays out
 * each rank's slice of the global u/v/w/vorticity_magnitude objects.
 *
 * curl_x = dw/dy - dv/dz
 * curl_y = du/dz - dw/dx
 * curl_z = dv/dx - du/dy
 *
 * Differences are central in the block interior and one-sided at the
 * block's own edges -- a stand-in for a halo exchange with neighboring
 * ranks, not a real one, and grid spacing is unit (dx=dy=dz=1). Like
 * vector_magnitude's plain sqrt(x^2+y^2+z^2), this is a benchmark
 * placeholder for curl's cost and data shape, not a scientifically exact
 * finite-volume/finite-difference discretization of E3SM's actual grid.
 */
#ifndef CURL_MATH_H
#define CURL_MATH_H

#include <stddef.h>

static inline double
curl_math_partial(const float *f, size_t idx, size_t stride, size_t extent, size_t coord)
{
    if (extent < 2)
        return 0.0;
    if (coord == 0)
        return (double)f[idx + stride] - (double)f[idx];
    if (coord == extent - 1)
        return (double)f[idx] - (double)f[idx - stride];
    return ((double)f[idx + stride] - (double)f[idx - stride]) / 2.0;
}

static inline void
curl_math_compute(const float *u, const float *v, const float *w, size_t nx, size_t ny, size_t nz,
                  double *curl_x, double *curl_y, double *curl_z)
{
    size_t stride_x = 1;
    size_t stride_y = nx;
    size_t stride_z = nx * ny;

    for (size_t k = 0; k < nz; k++) {
        for (size_t j = 0; j < ny; j++) {
            for (size_t i = 0; i < nx; i++) {
                size_t idx = (k * ny + j) * nx + i;

                double dwdy = curl_math_partial(w, idx, stride_y, ny, j);
                double dvdz = curl_math_partial(v, idx, stride_z, nz, k);
                double dudz = curl_math_partial(u, idx, stride_z, nz, k);
                double dwdx = curl_math_partial(w, idx, stride_x, nx, i);
                double dvdx = curl_math_partial(v, idx, stride_x, nx, i);
                double dudy = curl_math_partial(u, idx, stride_y, ny, j);

                curl_x[idx] = dwdy - dvdz;
                curl_y[idx] = dudz - dwdx;
                curl_z[idx] = dvdx - dudy;
            }
        }
    }
}

#endif /* CURL_MATH_H */
