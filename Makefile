ALL	:	top-traded.csv

sde.db	:	sde/fsd/typeIDs.yaml
	rm -f sde.db && ./build-sde.py --initial

top-traded.csv	:	popular.csv top-market-items.py  sde.db
	./top-market-items.py --exclude_category 2 4 5 17 25 42 43 65 91 > $@

sde-TRANQUILITY.zip	:
	curl -O https://eve-static-data-export.s3-eu-west-1.amazonaws.com/tranquility/sde.zip
	unzip $@

.DELETE_ON_ERROR	:	top-traded.tsv
