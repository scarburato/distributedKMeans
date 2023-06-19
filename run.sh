#!/bin/bash

STEPS=(1000 10000 100000 1000000 5000000 10000000 50000000 100000000)
K=3

echo "size, run, k, time, exit status" > results.csv
echo begin at $(date) > run.log

for t in "${STEPS[@]}"; do
	for run in {1..5}; do
		echo $t $run >> run.log

		/usr/bin/time -f "${t},${run},${K},%e,%x" -q -a -o results.csv \
			hadoop jar distributedKMeans-1.0-SNAPSHOT.jar unipi.cloudcomputing.KMeansMapReduce test \
			-i "/user/hadoop/project/bench/generated/clusters_${K}_cardinality_${t}.csv" \
			-o "/user/hadoop/project/output${t}_run${run}" \
			-d 6 \
			--clusters $K \
		>> run.log
	done
done
