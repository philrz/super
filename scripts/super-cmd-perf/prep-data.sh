#!/bin/bash -xv
set -euo pipefail
pushd "$(cd "$(dirname "$0")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Specify results directory string"
  exit 1
fi
rundir="$1"

if [ "$(uname)" = "Linux" ]; then
  cd /mnt
fi

mkdir gharchive_gz
cd gharchive_gz
for num in $(seq 0 23)
do
  wget "https://data.gharchive.org/2023-02-08-${num}.json.gz"
done
cd ..

time duckdb gha.db -c "CREATE TABLE gha AS FROM read_json('gharchive_gz/*.json.gz', union_by_name=true)" | tee "$rundir/duckdb-table-create.time" 2>&1
time duckdb gha.db -c "COPY (from gha) TO 'gha.parquet'" | tee "$rundir/duckdb-parquet-create.time" 2>&1
time super -o gha.bsup gharchive_gz/*.json.gz | tee "$rundir/super-bsup-create.time" 2>&1

du -h gha.db gha.parquet gha.bsup gharchive_gz
