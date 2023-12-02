top-traded.tsv	:	popular.csv top-market-items.py 
	./top-market-items.py --exclude_category 2 4 5 17 25 42 43 65 91 > $@

sde-TRANQUILITY.zip	:
	curl -O https://eve-static-data-export.s3-eu-west-1.amazonaws.com/tranquility/sde.zip

.DELETE_ON_ERROR	:	top-traded.tsv
