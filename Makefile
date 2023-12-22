ALL	:	top-traded.csv market-quality.csv market-history market-filler.csv

sde/fsd/typeIDs.yaml	:
	curl -O https://eve-static-data-export.s3-eu-west-1.amazonaws.com/tranquility/sde.zip
	unzip $<

sde.db	:	sde/fsd/typeIDs.yaml
	rm -f sde.db && ./build_sde.py --initial

top-traded.csv	:	latest-orderset-by-station-type.csv.gz popular.csv top_market_items.py  sde.db
	./top_market_items.py --orderset $< --exclude_category 2 4 5 17 25 42 43 65 91 > $@

sde-TRANQUILITY.zip	:
	curl -O https://eve-static-data-export.s3-eu-west-1.amazonaws.com/tranquility/sde.zip
	unzip $@

latest-orderset-by-station-type.csv.gz	:	latest.csv.gz
	zcat $< | sort -t '	'  -k 9n -k 2n | gzip -9 - > $@


market-quality.csv	:	latest-orderset-by-station-type.csv.gz top-traded.csv calc_market_quality.py
	./calc_market_quality.py --orderset $< --limit-top-traded-items 1000 | awk 'NR == 1; NR > 1 {print $0 | "sort -t , -k 3nr"}' > $@

bq-load	:
	bq load --source_format=CSV --null_marker - --skip_leading_rows=1 eve_markets.market_quality market-quality.csv market-efficiency-schema.json

market-history	:	latest-orderset-by-station-type.csv.gz top-traded.csv
	./add_orderset_to_market_history.py --orderset latest-orderset-by-station-type.csv.gz --filter_items top-traded.csv --extra_stations 1042137702248 60015180 60003166 1031058135975 1032792618788 60009928 1025824394754
	touch $@

latest-orderset	:
	curl https://market.fuzzwork.co.uk/api/orderset | jq '.orderset' > $@

latest.csv.gz	:	latest-orderset
	curl -O https://market.fuzzwork.co.uk/orderbooks/orderset-$$(cat $<).csv.gz
	ln -sf orderset-$$(cat $<).csv.gz $@

market-filler.csv	:	latest-orderset-by-station-type.csv.gz top-traded.csv industry-items.txt market-history
	python3 market_filler.py --orderset latest-orderset-by-station-type.csv.gz --from-stations 60003760 60011866 1025824394754 60003166 --limit-top-traded-items 1000 --station 60005686 --industry industry-items.txt > $@

tests	:
	python3 calc_market_quality_test.py
	python3 lib_test.py

.DELETE_ON_ERROR	:	top-traded.tsv market-history market-quality.csv
