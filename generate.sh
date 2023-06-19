#!/bin/bash

STEPS=(1_000 10_000 100_000 1_000_000 5_000_000 10_000_000 50_000_000)
CLUSTERS=3

for t in "${STEPS[@]}"; do
  echo "generated/clusters_${CLUSTERS}_cardinality_${t}.csv"
  python3.10 random_generator.py $t $CLUSTERS "generated/clusters_${CLUSTERS}_cardinality_${t}.csv"
done