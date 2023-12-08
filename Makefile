ALL	:	top-traded.csv market-quality.csv

sde.db	:	sde/fsd/typeIDs.yaml
	rm -f sde.db && ./build-sde.py --initial

top-traded.csv	:	popular.csv top-market-items.py  sde.db aggregatecsv.csv.gz
	./top-market-items.py --exclude_category 2 4 5 17 25 42 43 65 91 > $@

sde-TRANQUILITY.zip	:
	curl -O https://eve-static-data-export.s3-eu-west-1.amazonaws.com/tranquility/sde.zip
	unzip $@

latest-orderset-by-station-type.csv.gz	:	latest.csv.gz
	zcat $< | sort -t '	'  -k 9n -k 2n | gzip -9 - > $@


market-quality.csv	:	latest-orderset-by-station-type.csv.gz top-traded.csv
	zcat $< | ./calc-market-quality.py > $@

.DELETE_ON_ERROR	:	top-traded.tsv
