#!/bin/sh -e

# Prerequisites:
# comment out the dependency on latest-orderset for latest.csv.gz in Makefile
for x in backfill/orderset-*.gz; do
	n=$(basename "${x}"| sed -e 's/orderset-//' -e 's/.csv.gz//')
	echo "Processing orderset $n"
	target="backfill/market-efficiency-${n}.csv"
	if [ ! -f "${target}" ]; then
		ln -s "${x}" latest.csv.gz
		rm -f "latest-orderset-by-station-type.csv.gz"
		nice make market-efficiency.csv market-history bq-load
		mv -i market-efficiency.csv "${target}"
		rm latest.csv.gz market-history
	fi
done
