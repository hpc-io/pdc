set terminal pngcairo size 1600,1200 enhanced font 'Consolas,34'
set output 'data_md_zfp_40.png'

set xlabel "Number of Ranks"
set ylabel "Time (s)"

set yrange [0:*]
set xrange [0.8:8.2]

set xtics ("32" 1, "64" 2, "128" 3, "256" 4, "512" 5, "1024" 6, "62048" 7, "4096" 8)

set grid xtics ytics lw 8 lc rgb "black"
set grid mxtics mytics lw 3 lc rgb "#888888" dashtype 2

set key top left box lw 1 lc rgb "black"

set boxwidth 0.35
set style fill solid border rgb "black"

plot \
    'vpic_data_md_zfp_40.dat' using ($0+1-0.2):(0):2 with boxes lc rgb "blue" title "VPICIO Data", \
    '' using ($0+1-0.2):2:3 with boxes lc rgb "red" title "VPICIO Metadata", \
    'bdcats_data_md_zfp_40.dat' using ($0+1+0.2):(0):6 with boxes lc rgb "green" title "BDCATS Data", \
    '' using ($0+1+0.2):6:7 with boxes lc rgb "orange" title "BDCATS Metadata"