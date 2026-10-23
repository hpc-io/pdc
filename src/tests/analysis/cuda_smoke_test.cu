/**
 * Minimal standalone CUDA compilation/execution smoke test. Isolates "does
 * nvcc compilation and linking work in this CMake project" (the highest-risk,
 * first-of-its-kind part of adding a real GPU magnitude kernel to the
 * region-analysis framework) from the actual magnitude kernel's own logic.
 * Not linked against pdc/pdc_commons and does not start a PDC server -- this
 * is a pure CUDA runtime check.
 */
#include <cstdio>
#include <cstdlib>
#include <cuda_runtime.h>

__global__ void
add_vectors_kernel(const float *a, const float *b, float *out, int n)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n)
        out[i] = a[i] + b[i];
}

int
main(void)
{
    const int n     = 1024;
    const size_t bytes = (size_t)n * sizeof(float);

    float *h_a = (float *)malloc(bytes);
    float *h_b = (float *)malloc(bytes);
    float *h_out = (float *)malloc(bytes);
    for (int i = 0; i < n; i++) {
        h_a[i] = (float)i;
        h_b[i] = (float)(2 * i);
    }

    float *d_a, *d_b, *d_out;
    if (cudaMalloc(&d_a, bytes) != cudaSuccess || cudaMalloc(&d_b, bytes) != cudaSuccess ||
        cudaMalloc(&d_out, bytes) != cudaSuccess) {
        fprintf(stderr, "cudaMalloc failed\n");
        return 1;
    }

    cudaMemcpy(d_a, h_a, bytes, cudaMemcpyHostToDevice);
    cudaMemcpy(d_b, h_b, bytes, cudaMemcpyHostToDevice);

    int threads = 256;
    int blocks  = (n + threads - 1) / threads;
    add_vectors_kernel<<<blocks, threads>>>(d_a, d_b, d_out, n);

    cudaError_t launch_err = cudaGetLastError();
    if (launch_err != cudaSuccess) {
        fprintf(stderr, "kernel launch failed: %s\n", cudaGetErrorString(launch_err));
        return 1;
    }
    if (cudaDeviceSynchronize() != cudaSuccess) {
        fprintf(stderr, "cudaDeviceSynchronize failed\n");
        return 1;
    }

    cudaMemcpy(h_out, d_out, bytes, cudaMemcpyDeviceToHost);

    int ok = 1;
    for (int i = 0; i < n; i++) {
        float expected = h_a[i] + h_b[i];
        if (h_out[i] != expected) {
            fprintf(stderr, "mismatch at %d: got %f expected %f\n", i, h_out[i], expected);
            ok = 0;
            break;
        }
    }

    cudaFree(d_a);
    cudaFree(d_b);
    cudaFree(d_out);
    free(h_a);
    free(h_b);
    free(h_out);

    if (!ok)
        return 1;

    printf("cuda_smoke_test: PASS (%d elements)\n", n);
    return 0;
}
