# plots

Matplotlib comparison charts, one per workload family, each reading
directly from the `results_<mode>_*.csv` files the sbatch scripts in
`analysis/` / `transformation/` / `io/` produce -- no manual
copy/paste of numbers. All three:

- discover server count from each CSV row's own `servers_per_node`
  column (not directory names), so it doesn't matter which
  `<MM_DD_YYYY>-<jobid>/` output folder a file lives in;
- keep only the newest Slurm job ID's rows per (mode, server count,
  rank count) if a scale point was rerun;
- exclude verification/confirm-read time from every total -- see each
  script's own module docstring for the exact rationale (that time is a
  correctness check, not workload cost);
- require only `numpy` + `matplotlib` (no `pandas`).

| Script | Compares | Output |
|---|---|---|
| `plot_magnitude_comparison.py` | DF-eager, DF-view (lazy), PDC posthoc, HDF5 posthoc, HDF5 HighFive, ADIOS2 | `magnitude_comparison.png` |
| `plot_curl_comparison.py` | PDC eager (+ GPU-compressed variant), PDC posthoc, HDF5, ADIOS2 | `curl_comparison.png` |
| `plot_vpicio_comparison.py` | PDC raw, PDC + ZFP CPU, PDC + ZFP GPU, PDC + ZFP GPU+encrypt, ADIOS2 | `vpicio_comparison.png` |

Run any of them with no arguments from this directory to regenerate its
PNG in place:

```
python3 plot_magnitude_comparison.py
python3 plot_curl_comparison.py
python3 plot_vpicio_comparison.py
```

Each accepts `--*-root` flags to point at a different results location
and `--out` to write elsewhere -- see `--help` or the script's own
docstring for the exact flags and default paths.

`plot_magnitude_comparison.py` additionally takes `--server-count
{2,4,8}` to produce a single-server-count variant instead of the default
2/4/8-bracketed comparison: one plain bar per mode (no bracket, no
trend line -- both only make sense across multiple server counts), title
and output filename tagged with the count:

```
python3 plot_magnitude_comparison.py --server-count 2   # magnitude_comparison_2servers.png
python3 plot_magnitude_comparison.py --server-count 4   # magnitude_comparison_4servers.png
python3 plot_magnitude_comparison.py --server-count 8   # magnitude_comparison_8servers.png
```

`--server-count` also drops HDF5 HighFive from the default mode list
(pass `--modes` explicitly to override) -- these single-server-count
charts are a focused PDC-vs-HDF5-vs-ADIOS2 comparison, and HighFive is
the same underlying library as the HDF5 posthoc baseline already shown.
