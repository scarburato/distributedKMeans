#!/bin/bash

STEPS=(1000 10000 100000 500000 1000000 2000000 3000000 4000000 5000000 7500000 10000000)
CLUSTERS=(4 8 16)
D=12

for t in "${STEPS[@]}"; do
  for k in "${CLUSTERS[@]}"; do
    echo "generated/clusters_${k}_d${D}_cardinality_${t}.csv"
    time randomGen/a.out $k $t > "generated/clusters_${k}_d${D}_cardinality_${t}.csv"
  done

done
