

for f in /data/tmp/parquet/trip_data/trip_data_*.csv
do
	echo $cmd | mysql -u agger -pagger5129 test << EOF
		LOAD DATA INFILE "$f"
		INTO TABLE trip
		COLUMNS TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY '"'
		LINES TERMINATED BY '\r\n'
		IGNORE 1 LINES;
EOF

done
