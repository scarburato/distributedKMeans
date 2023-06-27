#!/bin/bash

D=(3 6 12)
K=(4 8 16)

# rg -o '\d+,3,4,.*' aggregated.csv --no-line-number

# variable size
for d in "${D[@]}"; do
	for k in "${K[@]}"; do
		head -n1 aggregated.csv > "data.${d}.${k}.csv"
		rg -o "\\d+,${d},${k},.*" --no-line-number aggregated.csv >> "data.${d}.${k}.csv"
	done
done

# variable k
for d in "${D[@]}"; do
	head -n1 aggregated.csv > "data.vark.${d}.csv"
	rg -o "4000000,${d},.*" --no-line-number aggregated.csv >> "data.vark.${d}.csv"

done
