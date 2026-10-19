set terminal pngcairo size 1600,1200 enhanced font 'Consolas,34'
set output 'combined_stacked.png'
set xlabel "Number of Ranks"
set ylabel "Time (s)"

set xtics ("32" 1, "64" 2, "128" 3, "256" 4, "512" 5, "1024" 6, "2048" 7, "4096" 8)
set xrange [0.5:8.5]
set yrange [0:*]

set mytics 5
set grid ytics mytics
set grid ytics lt 1 lw 1.5 lc rgb "black"
set grid mytics lt 1 lw 0.5 lc rgb "black"

set key above horizontal box lt 1 lc rgb "#aaaaaa" lw 1 spacing 1.2
set key Left reverse
set bmargin 4

BarWidth = 0.2
set boxwidth BarWidth
set style fill solid

node_index(n) = \
    (n==1)   ? 1 : \
    (n==2)   ? 2 : \
    (n==4)   ? 3 : \
    (n==8)   ? 4 : \
    (n==16)  ? 5 : \
    (n==32)  ? 6 : \
    (n==64)  ? 7 : \
    (n==128) ? 8 : 0

x1(n) = node_index(n) - 0.3
x2(n) = node_index(n) - 0.1
x3(n) = node_index(n) + 0.1
x4(n) = node_index(n) + 0.3

plot \
    'vpic_hdf5_joined.dat'   using (x1(column(1))):((column(2)+column(4))*4 + column(7)) with boxes lc rgb "#2196F3" fc rgb "#2196F3" fs pattern 2 border -1 title "HDF5 VPIC outlier", \
    'vpic_hdf5_joined.dat'   using (x1(column(1))):((column(2)+column(4))*4)             with boxes lc rgb "#2196F3" fc rgb "#2196F3" fs solid border -1 title "HDF5 VPIC 4-step", \
    'vpic_joined.dat'        using (x2(column(1))):((column(2)+column(4))*4 + column(7)) with boxes lc rgb "#F44336" fc rgb "#F44336" fs pattern 2 border -1 title "PDC VPIC outlier", \
    'vpic_joined.dat'        using (x2(column(1))):((column(2)+column(4))*4)             with boxes lc rgb "#F44336" fc rgb "#F44336" fs solid border -1 title "PDC VPIC 4-step", \
    'bdcats_hdf5_joined.dat' using (x3(column(1))):((column(2)+column(4))*4 + column(7)) with boxes lc rgb "#4CAF50" fc rgb "#4CAF50" fs pattern 2 border -1 title "HDF5 BDCATS outlier", \
    'bdcats_hdf5_joined.dat' using (x3(column(1))):((column(2)+column(4))*4)             with boxes lc rgb "#4CAF50" fc rgb "#4CAF50" fs solid border -1 title "HDF5 BDCATS 4-step", \
    'bdcats_joined.dat'      using (x4(column(1))):((column(2)+column(4))*4 + column(7)) with boxes lc rgb "#FF9800" fc rgb "#FF9800" fs pattern 2 border -1 title "PDC BDCATS outlier", \
    'bdcats_joined.dat'      using (x4(column(1))):((column(2)+column(4))*4)             with boxes lc rgb "#FF9800" fc rgb "#FF9800" fs solid border -1 title "PDC BDCATS 4-step", \
    'vpic_hdf5_joined.dat'   using (x1(column(1))):((column(2)+column(4))*4 + column(7)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'vpic_joined.dat'        using (x2(column(1))):((column(2)+column(4))*4 + column(7)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_hdf5_joined.dat' using (x3(column(1))):((column(2)+column(4))*4 + column(7)) with boxes lw 3 lc rgb "black" fs empty notitle, \
    'bdcats_joined.dat'      using (x4(column(1))):((column(2)+column(4))*4 + column(7)) with boxes lw 3 lc rgb "black" fs empty notitle