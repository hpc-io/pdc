set terminal pngcairo size 1600,1200 enhanced font 'Consolas,34'
set output 'zfp_gpu.png'

set xlabel "Number of elements"
set ylabel "Time (ms)"

# Key
set key above horizontal box lt 1 lc rgb "#aaaaaa" lw 1 spacing 1.2
set key Left reverse
set bmargin 4

# Grid
set mytics 5
set grid ytics mytics
set grid ytics  lt 1 lw 1.5 lc rgb "black"
set grid mytics lt 1 lw 0.5 lc rgb "black"

set yrange [0:*]
set xtics rotate by -45

# Stacked histogram style
set style data histograms
set style histogram rowstacked
set boxwidth 0.8
set autoscale xfixmin

set style line 1 lc rgb "#1f77b4" lt 1 lw 3
set style line 2 lc rgb "#f0a500" lt 1 lw 3
set style line 3 lc rgb "#d62728" lt 1 lw 3

plot \
    'zfp_metrics.dat' using 2:xtic(1) title "CPU->GPU"    ls 1 fill solid border -1, \
    '' using 3                         title "Compute ZFP" ls 2 fill solid border -1, \
    '' using 4                         title "GPU->CPU"    ls 3 fill solid border -1