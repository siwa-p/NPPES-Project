#!/bin/bash

echo "Starting data pipeline... "

BASE_URL="http://localhost:7071/api"

for file in nppes nucc fips zip county_pop; do
    echo " Processing $file next ..."
    curl "${BASE_URL}/load_nppes?file=$file"
    sleep 5
done

echo "Running stored procs now !"

curl "${BASE_URL}/parse_records"

echo "Pipeline completed!"