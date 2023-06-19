#!/bin/bash

STEPS=(1_000 10_000 100_000 1_000_000 5_000_000 10_000_000 50_000_000)
K=3

for t in "${STEPS[@]}"; do
  echo $t >> run.log

  /usr/bin/time -f "${t},%e,%x" -q -a -o results.csv \
  hadoop jar distributedKMeans-1.0-SNAPSHOT.jar unipi.cloudcomputing.KMeansMapReduce test \
    -i "/user/hadoop/project/bench/generated/clusters_${K}_cardinality_${t}.csv" \
    -o "/user/hadoop/project/output${t}_run0" \
    -d 4 \
  >> run.log
done