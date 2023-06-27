#!/usr/bin/gnuplot -persist

set datafile separator ','
plot 'test.csv' using 2:3:(int($1)+1) lc variable
