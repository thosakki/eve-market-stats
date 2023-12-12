#!/bin/sh -e

for x in backfill/*; do
	n=$(basename "${x}"| sed -e 's/orderset-//' -e 's/.csv.gz//')
	target="backfill/market-quality-${n}.csv"
	if [ ! -f "${target}" ]; then
		ln -s "${x}" latest.csv.gz
		rm -f "latest-orderset-by-station-type.csv.gz"
		nice make
		mv -i market-quality.csv "${target}"
		rm latest.csv.gz
	fi
done
