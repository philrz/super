#!/bin/bash
set -euo pipefail
pushd "$(cd "$(dirname "$0")" && pwd)"

if [ "$#" -ne 1 ]; then
  echo "Specify results directory string"
  exit 1
fi
rundir="$(pwd)/$1"
mkdir -p "$rundir"

if [ "$(uname)" = "Linux" ]; then
  storage="/mnt/"
else
  storage=""
fi

warmups=1
runs=1
report="$rundir/report_$(basename "$rundir").md"

function run_query {
  cmd="$1"
  shift
  queryfile="$1"
  shift
  source="$1"
  shift
  outputfile="$rundir/$cmd-$queryfile-$source.out"
  timefile="$rundir/$cmd-$queryfile-$source.time"

  final_query=$(mktemp)
  if [ "$source" == "gha" ]; then
    sed -e "s/__SOURCE__/$source/" "queries/$queryfile" > "$final_query"
  else
    sed -e "s/__SOURCE__/${storage//\//\\/}${source}/" "queries/$queryfile" > "$final_query"
  fi

  if [ "$cmd" == "super" ]; then
    if [ "$source" == "gha.parquet" ]; then
      cmd="SUPER_VAM=1 super"
    fi
    cmd="$cmd -z -I $final_query"
  elif [ "$cmd" == "duckdb" ]; then
    if [ "$source" == "gha" ]; then
      cmd="duckdb ${storage}gha.db"
    fi
    cmd="$cmd < $final_query"
  elif [ "$cmd" == "datafusion" ]; then
    cmd="datafusion-cli --file $final_query"
  elif [ "$cmd" == "clickhouse" ]; then
    cmd="clickhouse --queries-file $final_query"
  fi

  echo -e "About to execute: $cmd\n\nwith query:" > "$timefile"
  cat "$final_query" >> "$timefile"
  echo >> "$timefile"

  { hyperfine \
      --output "$outputfile" \
      --warmup $warmups \
      --runs $runs \
      --time-unit second \
      "$cmd" ;
  } \
    >> "$timefile" \
    2>&1

  rm -f "$final_query"
}

echo "|**Tool**|**Format**|**search**|**search+**|**count**|**agg**|**union**|" > "$report"
echo "|-|-|-|-|-|-|-|" >> "$report"

for source in gha.bsup gha.parquet
do
  echo -n "|\`super\`|\`${source/gha./}\`|" >> "$report"
  for queryfile in search.spq search+.spq count.sql agg.sql union.spq
  do
    if [ "$source" == "gha.parquet" ] && { [ "$queryfile" == "search.spq" ] || [ "$queryfile" == "search+.spq" ] || [ "$queryfile" == "union.spq" ]; }; then
      echo -n "N/A|" >> "$report"
      continue
    fi
    run_query super $queryfile "$source"
    echo -n "$(grep Time < "$rundir/super-$queryfile-$source.time" | awk '{ print $4 }')" >> "$report"
    echo -n "|" >> "$report"
  done
  echo >> "$report"
done

for source in gha gha.parquet
do
  duckdb_source=${source/gha\./}
  duckdb_source=${duckdb_source/gha/db}
  echo -n "|\`duckdb\`|\`$duckdb_source\`|" >> "$report"
  for queryfile in search.sql search+.sql count.sql agg.sql union.sql
  do
    run_query duckdb $queryfile "$source"
    echo -n "$(grep Time < "$rundir/duckdb-$queryfile-$source.time" | awk '{ print $4 }')" >> "$report"
    echo -n "|" >> "$report"
  done
  echo >> "$report"
done

echo -n "|\`datafusion\`|\`parquet\`|" >> "$report"
for queryfile in search.sql search+.sql count.sql agg.sql union-datafusion.sql
do
  run_query datafusion $queryfile gha.parquet
  echo -n "$(grep Time < "$rundir/datafusion-$queryfile-$source.time" | awk '{ print $4 }')" >> "$report"
  echo -n "|" >> "$report"
done
echo >> "$report"

echo -n "|\`clickhouse\`|\`parquet\`|" >> "$report"
for queryfile in search.sql search+.sql count.sql agg.sql union-clickhouse.sql
do
  run_query clickhouse $queryfile gha.parquet
  echo -n "$(grep Time < "$rundir/clickhouse-$queryfile-$source.time" | awk '{ print $4 }')" >> "$report"
  echo -n "|" >> "$report"
done
echo >> "$report"
