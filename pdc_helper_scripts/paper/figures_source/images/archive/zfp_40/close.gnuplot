set terminal pngcairo size 1600,1200 enhanced font 'Consolas,34'
set output 'close_zfp_40.png'
set xlabel "Number of Ranks"
set ylabel "Time (s)"

# X-axis
set xtics ("32" 1, "64" 2, "128" 3, "256" 4, "512" 5, "1024" 6, "62048" 7, "4096" 8)
set xrange [0.5:8.5]
set yrange [*:*]

# Log scale
set logscale y
set format y "%g"

# Grid
set mytics 5
set grid ytics mytics
set grid ytics  lt 1 lw 1.5 lc rgb "black"
set grid mytics lt 1 lw 0.5 lc rgb "black"

# Key
set key above horizontal box lt 1 lc rgb "#aaaaaa" lw 1 spacing 1.2
set key Left reverse
set bmargin 4

# Bar layout: 4 touching bars per cluster
BarWidth = 0.2
set boxwidth BarWidth
set style fill solid

node_index(n) = \
    (n==1)  ? 1 : \
    (n==2)  ? 2 : \
    (n==4)  ? 3 : \
    (n==8)  ? 4 : \
    (n==16) ? 5 : \
    (n==32) ? 6 : \
    (n==64) ? 7 : \
    (n==128)? 8 : 0

x1(n) = node_index(n) - 0.3   # VPIC 40
x2(n) = node_index(n) - 0.1   # VPIC 100
x3(n) = node_index(n) + 0.1   # BDCATS 40
x4(n) = node_index(n) + 0.3   # BDCATS 100

# Columns: (1)nodes (2)avg_40 (3)stdev_40 (4)avg_100 (5)stdev_100
plot \
    'vpic_close_zfp_40.dat'   using (x1(column(1))):(column(2)) with boxes lc rgb "#1f77b4" fc rgb "#1f77b4" fs solid border -1 title "VPIC Close 40",   \
    'vpic_close_zfp_40.dat'   using (x2(column(1))):(column(4)) with boxes lc rgb "#08306b" fc rgb "#08306b" fs solid border -1 title "VPIC Close 100",  \
    'bdcats_close_zfp_40.dat' using (x3(column(1))):(column(2)) with boxes lc rgb "#d62728" fc rgb "#d62728" fs solid border -1 title "BDCATS Close 40", \
    'bdcats_close_zfp_40.dat' using (x4(column(1))):(column(4)) with boxes lc rgb "#d97a2a" fc rgb "#d97a2a" fs solid border -1 title "BDCATS Close 100",\
    'vpic_close_zfp_40.dat'   using (x1(column(1))):(column(2)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'vpic_close_zfp_40.dat'   using (x2(column(1))):(column(4)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_close_zfp_40.dat' using (x3(column(1))):(column(2)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_close_zfp_40.dat' using (x4(column(1))):(column(4)) with boxes lw 3 lc rgb "black" fs empty notitle