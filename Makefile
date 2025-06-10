ALL	:	top-traded.csv market-history market-filler-tar.csv market-filler-dodixie.csv

reset	:
	rm -f $(assets) $(orders) latest-orderset

sde/fsd/types.yaml	:
	wget https://eve-static-data-export.s3-eu-west-1.amazonaws.com/tranquility/sde.zip
	unzip sde.zip -d sde

sde.db	:	sde/fsd/types.yaml
	rm -f sde.db && ./build_sde.py --initial

top-traded.csv	:	popular*.csv top_market_items.py order-sizes.txt sde.db
	./top_market_items.py --exclude_category 2 4 5 9 17 25 41 42 43 65 91 2118 --popular popular*.csv > $@

latest-orderset-by-station-type.csv.gz	:	latest.csv.gz
	zcat $< | sort -t '	'  -k 9n -k 2n | gzip -9 - > $@

market-efficiency.csv	:	latest-orderset-by-station-type.csv.gz top-traded-measure.csv calc_market_quality.py
	./calc_market_quality.py --orderset $< --top-traded-items top-traded-measure.csv --limit-top-traded-items 1000 | awk 'NR == 1; NR > 1 {print $0 | "sort -t , -k 3nr"}' > $@

bq-load	:	market-efficiency.csv
	bq load --source_format=CSV --null_marker - --skip_leading_rows=1 eve_markets.market_efficiency $< market-efficiency-schema.json

market-history	:	latest-orderset-by-station-type.csv.gz top-traded.csv top-traded-measure.csv industry-items.csv
	./add_orderset_to_market_history.py --orderset latest-orderset-by-station-type.csv.gz --filter_items top-traded.csv top-traded-measure.csv industry-items.csv --extra_stations 1042137702248 60015180 60003166 1031058135975 1032792618788 60009928 1025824394754 60012739
	touch $@

latest-orderset	:
	wget -O - https://market.fuzzwork.co.uk/api/orderset | jq '.orderset' > $@

latest.csv.gz	:	latest-orderset
	wget -N https://market.fuzzwork.co.uk/orderbooks/orderset-$$(cat $<).csv.gz
	ln -sf orderset-$$(cat $<).csv.gz $@

assets-corporation.csv	:
	(cd esi && ./get-assets.py --character $$(cat corporation.character) --corporation $$(cat corporation.corporation) ) > $@

assets-%.csv	:	esi/state-%.yaml
	(cd esi && ./get-assets.py --character $(patsubst esi/state-%.yaml,%,$^)) > $@

assets = $(patsubst esi/state-%.yaml,assets-%.csv,$(wildcard esi/state-*.yaml)) assets-corporation.csv

orders-%.csv	:	esi/state-%.yaml
	(cd esi && ./get-orders.py --character $(patsubst esi/state-%.yaml,%,$^)) > $@

orders = $(patsubst esi/state-%.yaml,orders-%.csv,$(wildcard esi/state-*.yaml))

market-filler-dodixie.csv	:	latest.csv.gz top-traded.csv industry.db market-history $(assets) $(orders)
	python3 market_filler.py --top-traded-items top-traded.csv --orderset latest.csv.gz --station Dodixie --sources sources.yaml --limit-top-traded-items 1000 --assets $(assets) --orders $(orders) --exclude_industry exclude-industry.txt --stock_fraction 0.04 > $@

market-filler-tar.csv	:	latest.csv.gz top-traded.csv industry.db market-history $(assets) $(orders)
	python3 market_filler.py --top-traded-items top-traded.csv --orderset latest.csv.gz --limit-top-traded-items 850 --station Tar --sources sources.yaml --assets $(assets) --orders $(orders) --exclude_industry exclude-industry.txt --exclude_market_paths exclude-market-tar.txt > $@

industry-items.csv	:	industry.db
	./list-industry-inputs-outputs.py > $@

industry.db	:	industry-inputs.csv sde.db
	./load-industry.py --industry industry-inputs.csv

top-traded-measure.1000	:	top-traded-measure.csv
	./top-1000.sh < $< > $@

top-traded.1000	:	top-traded.csv
	./top-1000.sh < $< > $@

tests	:
	python3 calc_market_quality_test.py
	python3 lib_test.py
	python3 market_filler_test.py

.DELETE_ON_ERROR	:	top-traded.tsv market-history market-quality.csv
