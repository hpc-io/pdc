set terminal pngcairo size 1600,1200 enhanced font 'Consolas,34'
set output 'data_md_raw.png'
set xlabel "Number of Ranks"
set ylabel "Time (s)"

# X-axis
set xtics ("32" 1, "64" 2, "128" 3, "256" 4, "512" 5, "1024" 6, "2048" 7, "4096" 8)
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

# Bar layout: PDC VPIC, HDF5 VPIC, PDC BDCATS, HDF5 BDCATS
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

x1(n) = node_index(n) - 0.3   # PDC VPIC
x2(n) = node_index(n) - 0.1   # HDF5 VPIC
x3(n) = node_index(n) + 0.1   # PDC BDCATS
x4(n) = node_index(n) + 0.3   # HDF5 BDCATS

LabelOffset = 0.2

# Speedup labels for nodes 64 and 128 only (above PDC bars)
set label "9.5x"  at x1(64),  1.3570 rotate by 90 left font 'Consolas,22' textcolor rgb "#08306b" offset 0,LabelOffset
set label "21.5x" at x1(128), 1.3590 rotate by 90 left font 'Consolas,22' textcolor rgb "#08306b" offset 0,LabelOffset

# Columns: (1)nodes (2)avg_data (3)stdev_data (4)avg_metadata (5)stdev_metadata
plot \
    'vpic_data_md_raw.dat'        using (x1(column(1))):(column(2)+column(4)) with boxes lc rgb "#08306b" fc rgb "#08306b" fs pattern 4 border -1 title "PDC VPIC metadata",    \
    'vpic_data_md_raw.dat'        using (x1(column(1))):(column(2))           with boxes lc rgb "#1f77b4" fc rgb "#1f77b4" fs solid      border -1 title "PDC VPIC data",        \
    'vpic_data_md_raw_hdf5.dat'   using (x2(column(1))):(column(2)+column(4)) with boxes lc rgb "#3f007d" fc rgb "#3f007d" fs pattern 4 border -1 title "HDF5 VPIC metadata",   \
    'vpic_data_md_raw_hdf5.dat'   using (x2(column(1))):(column(2))           with boxes lc rgb "#9e4bc4" fc rgb "#9e4bc4" fs solid      border -1 title "HDF5 VPIC data",       \
    'bdcats_data_md_raw.dat'      using (x3(column(1))):(column(2)+column(4)) with boxes lc rgb "#67000d" fc rgb "#67000d" fs pattern 4 border -1 title "PDC BDCATS metadata",  \
    'bdcats_data_md_raw.dat'      using (x3(column(1))):(column(2))           with boxes lc rgb "#d62728" fc rgb "#d62728" fs solid      border -1 title "PDC BDCATS data",      \
    'bdcats_data_md_raw_hdf5.dat' using (x4(column(1))):(column(2)+column(4)) with boxes lc rgb "#7f2704" fc rgb "#7f2704" fs pattern 4 border -1 title "HDF5 BDCATS metadata", \
    'bdcats_data_md_raw_hdf5.dat' using (x4(column(1))):(column(2))           with boxes lc rgb "#d97a2a" fc rgb "#d97a2a" fs solid      border -1 title "HDF5 BDCATS data",     \
    'vpic_data_md_raw.dat'        using (x1(column(1))):(column(2)+column(4)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'vpic_data_md_raw.dat'        using (x1(column(1))):(column(2))           with boxes lw 3 lc rgb "black" fs empty notitle, \
    'vpic_data_md_raw_hdf5.dat'   using (x2(column(1))):(column(2)+column(4)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'vpic_data_md_raw_hdf5.dat'   using (x2(column(1))):(column(2))           with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_data_md_raw.dat'      using (x3(column(1))):(column(2)+column(4)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_data_md_raw.dat'      using (x3(column(1))):(column(2))           with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_data_md_raw_hdf5.dat' using (x4(column(1))):(column(2)+column(4)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_data_md_raw_hdf5.dat' using (x4(column(1))):(column(2))           with boxes lw 3 lc rgb "black" fs empty notitle