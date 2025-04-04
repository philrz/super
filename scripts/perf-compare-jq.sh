#!/bin/bash
# shellcheck disable=SC2016    # The backticks in quotes are for markdown, not expansion

set -eo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
cd "$script_dir" || exit 1

wget https://data.gharchive.org/2023-02-08-0.json.gz
gunzip 2023-02-08-0.json.gz
data="2023-02-08-0"

for cmd in super jq; do
  if ! [[ $(type -P "$cmd") ]]; then
    echo "$cmd not found in PATH"
    exit 1
  fi
done

declare -a markdowns=(
  '00_all_unmodified.md'
  '01_search.md'
  '02_search+.md'
  '03_count.md'
  '04_agg.md'
  '05_union.md'
)

declare -a descriptions=(
  'Output all events unmodified'
  'Search'
  'Search+'
  'Count'
  'Agg'
  'Union'
)

declare -a superdb_queries=(
  "pass"
  "SELECT count() FROM '__SOURCE__' WHERE grep('in case you have any feedback 😊', payload.pull_request.body)"
  "SELECT count() FROM '__SOURCE__' WHERE grep('in case you have any feedback 😊')"
  "SELECT count() FROM '__SOURCE__' WHERE actor.login='johnbieren'"
  "SELECT count(),type FROM '__SOURCE__' WHERE repo.name='duckdb/duckdb' GROUP BY type"
  "FROM '__SOURCE__' | UNNEST [...payload.pull_request.assignees, payload.pull_request.assignee] | WHERE this IS NOT NULL | AGGREGATE count() BY assignee:=login | ORDER BY count DESC | LIMIT 5"
)

declare -a jq_filters=(
  '.'
  '[.[] | select((.payload.pull_request.body // "") | contains("in case you have any feedback 😊"))] | length'
  '[.[] | select(to_entries | any(.value | tostring | contains("in case you have any feedback 😊")))] | length'
  '[.[] | select(.actor.login == "johnbieren")] | length'
  '.[] | [select(.repo.name == "duckdb/duckdb")] | group_by(.type)[] | length as $l | .[0] | .count = $l | {count,"type"}'
  '[.[] | (.payload.pull_request.assignee.login // empty, .payload.pull_request.assignees[]?.login // empty)] | group_by(.) | map({assignee: .[0], count: length}) | sort_by(.count) | reverse | .[:5] | .[]'
)

declare -a jq_flags=(
  '-c'
  '-c -s'
  '-c -s'
  '-c -s'
  '-c -s'
)

for (( n=0; n<"${#superdb_queries[@]}"; n++ )); do
  desc=${descriptions[$n]}
  md=${markdowns[$n]}
  echo -e "### $desc\n" | tee "$md"
  echo "|**<br>Tool**|**<br>Arguments**|**Input<br>Format**|**Output<br>Format**|**<br>Real**|**<br>User**|**<br>Sys**|" | tee -a "$md"
  echo "|:----------:|:---------------:|:-----------------:|:------------------:|-----------:|-----------:|----------:|" | tee -a "$md"
  for input_format in json sup bsup csup; do
    for output_format in json sup bsup csup; do
      if [ $n -gt 0 ] && [ $output_format != "json" ]; then
        continue
      fi
      superdb_query=${superdb_queries[$n]}
      echo -n "|\`super\`|\`$superdb_query\`|$input_format|$output_format|" | tee -a "$md"
      if [ $input_format = "json" ]; then
        input_file="${data}.json"
      else
        input_file="$TMPDIR/${data}.${input_format}"
      fi
      output_file=$(mktemp)
      all_times=$(mktemp)
      time -p super -i $input_format -f $output_format -c "$superdb_query" "$input_file" > "$output_file" 2> "$all_times"
      < "$all_times" tr '\n' ' ' | awk '{ print $2 "|" $4 "|" $6 "|" }' | tee -a "$md"
      rm "$all_times"
      mv "$output_file" "$TMPDIR/${data}.${output_format}"
    done
  done

  jq_query=${jq_filters[$n]}
  jq_flag=${jq_flags[$n]}
  echo -n "|\`jq\`|\`$jq_flag ""'""${jq_query//|/\\|}""'""\`|json|json|" | tee -a "$md"
  all_times=$(mktemp)
  # shellcheck disable=SC2086      # For expanding JQ_FLAG
  time -p jq $jq_flag "$jq_query" "${data}.json" > /dev/null 2>&1 "$all_times"
  < "$all_times" tr '\n' ' ' | awk '{ print $2 "|" $4 "|" $6 "|" }' | tee -a "$md"

  echo | tee -a "$md"
done
