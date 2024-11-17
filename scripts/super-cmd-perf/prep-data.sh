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

function run_cmd {
  outputfile="$1"
  shift
  timefile="$1"
  shift
  { hyperfine \
      --output "$outputfile" \
      --warmup 0 \
      --runs 1 \
      --time-unit second \
      "$@" ;
  } \
    > "$timefile" \
    2>&1
}

mkdir gharchive_gz
cd gharchive_gz
for num in $(seq 0 23)
do
  wget "https://data.gharchive.org/2023-02-08-${num}.json.gz"
done
cd ..

run_cmd \
  "$rundir/duckdb-table-create.out" \
  "$rundir/duckdb-table-create.time" \
  "duckdb gha.db -c \"CREATE TABLE gha AS FROM read_json('gharchive_gz/*.json.gz', union_by_name=true)\""

run_cmd \
  "$rundir/duckdb-parquet-create.out" \
  "$rundir/duckdb-parquet-create.time" \
  "duckdb gha.db -c \"COPY (from gha) TO 'gha.parquet'\""

run_cmd \
  "$rundir/super-bsup-create.out" \
  "$rundir/super-bsup-create.time" \
  "super -o gha.bsup gharchive_gz/*.json.gz"

du -h gha.db gha.parquet gha.bsup gharchive_gz
