#!/bin/bash

STEPS=(1000 10000 100000 1000000 5000000 10000000 50000000 100000000)
CLUSTERS=3

for t in "${STEPS[@]}"; do
  echo "generated/clusters_${CLUSTERS}_cardinality_${t}.csv"
  #python3.10 random_generator.py $t $CLUSTERS "generated/clusters_${CLUSTERS}_cardinality_${t}.csv"
  time randomGen/a.out $CLUSTERS $t > "generated/clusters_${CLUSTERS}_cardinality_${t}.csv"

done
