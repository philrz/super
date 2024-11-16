#!/bin/bash -xv
set -euo pipefail
pushd "$(cd "$(dirname "$0")" && pwd)"
source "$HOME"/.profile
cd /mnt

mkdir gharchive_gz
cd gharchive_gz
for num in $(seq 0 23)
do
  wget "https://data.gharchive.org/2023-02-08-${num}.json.gz"
done
cd ..

time duckdb gha.db -c "CREATE TABLE gha AS FROM read_json('gharchive_gz/*.json.gz', union_by_name=true)"
time duckdb gha.db -c "COPY (from gha) TO 'gha.parquet'"
time super gharchive_gz/*.json.gz > gha.bsup

du -h gha.db gha.parquet gha.bsup gharchive_gz
