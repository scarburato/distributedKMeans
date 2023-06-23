#!/bin/bash

STEPS=(1000 10000 100000 500000 1000000 2000000 3000000 4000000 5000000 7500000 10000000)
K=(4 8 16)
D=(3 6 12)

echo "size,run,k,d,time,start time,exit status" >results.csv
echo begin at "$(date --iso-8601=seconds)" >run.log

for t in "${STEPS[@]}"; do
  for d in "${D[@]}"; do
    for k in "${K[@]}"; do
      for run in {1..12}; do
        echo $t $d $k $run >> run.log

        # Delete of file, if exists
        hadoop fs -rm -r "/user/hadoop/project/output${t}_d${d}_run${run}"

        /usr/bin/time -f "${t},${run},${k},${d},$(date --iso-8601=seconds),%e,%x" -q -a -o results.csv \
            hadoop jar distributedKMeans-1.0-SNAPSHOT.jar unipi.cloudcomputing.KMeansMapReduce test \
            -i "/user/hadoop/project/bench/generated/clusters_${k}_d${d}_cardinality_${t}.csv" \
            -o "/user/hadoop/project/output${t}_d${d}_run${run}" \
            -d ${d} \
            --clusters $k \
            --maxiterations 11 \
            --threshold 0.005 \
          >> run.log
      done
    done
  done
done
