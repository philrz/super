#!/bin/bash -xv
set -euo pipefail
pushd "$(cd "$(dirname "$0")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Specify results directory string"
  exit 1
fi
rundir="$(pwd)/$1"
mkdir -p "$rundir"

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

time duckdb gha.db -c "CREATE TABLE gha AS FROM read_json('gharchive_gz/*.json.gz', union_by_name=true)" 2>&1 | tee "$rundir/duckdb-table-create.time"
time duckdb gha.db -c "COPY (from gha) TO 'gha.parquet'" 2>&1 | tee "$rundir/duckdb-parquet-create.time"
time super -o gha.bsup gharchive_gz/*.json.gz 2>&1 | tee "$rundir/super-bsup-create.time"

du -h gha.db gha.parquet gha.bsup gharchive_gz
