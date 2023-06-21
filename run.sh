#!/bin/bash

#STEPS=(1000 10000 100000 1000000 5000000 10000000 50000000 100000000)
#STEPS=(3000000 4000000 2000000 200000000)
STEPS=(1000 10000 100000 1000000 2000000 3000000 4000000 5000000 10000000)
K=3
D=12

echo "size,run,k,d,time,exit status" > results.csv
echo begin at $(date) > run.log

for t in "${STEPS[@]}"; do
        for run in {1..3}; do
                echo $t $run >> run.log

                # Delete of file, if exists
                hadoop fs -rm -r "/user/hadoop/project/output${t}_d${D}_run${run}"

                /usr/bin/time -f "${t},${run},${K},${D},%e,%x" -q -a -o results.csv \
                        hadoop jar distributedKMeans-1.0-SNAPSHOT.jar unipi.cloudcomputing.KMeansMapReduce test \
                        -i "/user/hadoop/project/bench/generated/clusters_${K}_d${D}_cardinality_${t}.csv" \
                        -o "/user/hadoop/project/output${t}_d${D}_run${run}" \
                        -d ${D} \
                        --clusters $K \
                >> run.log
        done
done
