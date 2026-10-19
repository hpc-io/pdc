set terminal pngcairo size 1600,1200 enhanced font 'Consolas,34'
set output 'data_md.png'

node_index(n) = \
    (n==1)  ? 1 : \
    (n==2)  ? 2 : \
    (n==4)  ? 3 : \
    (n==8)  ? 4 : \
    (n==16) ? 5 : \
    (n==32) ? 6 : \
    (n==64) ? 7 : \
    (n==128)? 8 : 0

BarWidth = 0.35
set boxwidth BarWidth
set style fill solid

xvpic(n)   = node_index(n) - 0.175
xbdcats(n) = node_index(n) + 0.175

set xtics ('32' 1, '64' 2, '128' 3, '256' 4, '512' 5, '1024' 6, '2048' 7, '4096' 8)
set xrange [0.5:8.5]
set yrange [0.01:*]

set xlabel "Ranks"
set ylabel "Time (s)"
set bmargin 4

# Log scale
set logscale y
set format y "%g"

# Key
set key above horizontal box lt 1 lc rgb "#aaaaaa" lw 1 spacing 1.2
set key Left reverse

# Grid
set mytics 5
set grid ytics mytics
set grid ytics  lt 1 lw 1.5 lc rgb "black"
set grid mytics lt 1 lw 0.5 lc rgb "black"

# Columns: (1)nodes (2)avg_data (3)stdev_data (4)avg_metadata (5)stdev_metadata
plot \
    'vpic_data_md_raw.dat'   using (xvpic(column(1))):(column(2)+column(4))   with boxes lc rgb "#08306b" fc rgb "#08306b" fs pattern 4 border -1 title "VPICIO metadata",  \
    'vpic_data_md_raw.dat'   using (xvpic(column(1))):(column(2))             with boxes lc rgb "#1f77b4" fc rgb "#1f77b4" fs solid      border -1 title "VPICIO data",      \
    'bdcats_data_md_raw.dat' using (xbdcats(column(1))):(column(2)+column(4)) with boxes lc rgb "#67000d" fc rgb "#67000d" fs pattern 4 border -1 title "BDCATS metadata",  \
    'bdcats_data_md_raw.dat' using (xbdcats(column(1))):(column(2))           with boxes lc rgb "#d62728" fc rgb "#d62728" fs solid      border -1 title "BDCATS data",      \
    'vpic_data_md_raw.dat'   using (xvpic(column(1))):(column(2)+column(4))   with boxes lw 3 lc rgb "black" fs empty notitle, \
    'vpic_data_md_raw.dat'   using (xvpic(column(1))):(column(2))             with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_data_md_raw.dat' using (xbdcats(column(1))):(column(2)+column(4)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_data_md_raw.dat' using (xbdcats(column(1))):(column(2))           with boxes lw 3 lc rgb "black" fs empty notitle