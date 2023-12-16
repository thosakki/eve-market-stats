#!/bin/sh -e

for x in backfill/orderset-*.gz; do
	n=$(basename "${x}"| sed -e 's/orderset-//' -e 's/.csv.gz//')
	echo "Processing orderset $n"
	target="backfill/market-quality-${n}.csv"
	if [ ! -f "${target}" ]; then
		ln -s "${x}" latest.csv.gz
		rm -f "latest-orderset-by-station-type.csv.gz"
		nice make market-quality.csv market-history
		mv -i market-quality.csv "${target}"
		rm latest.csv.gz
	fi
done
