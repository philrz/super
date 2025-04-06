#!/bin/bash
# shellcheck disable=SC2016    # The backticks in quotes are for markdown, not expansion

set -eo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
cd "$script_dir" || exit 1

wget https://data.gharchive.org/2023-02-08-0.json.gz
gunzip -f 2023-02-08-0.json.gz
data="2023-02-08-0"
md="report.md"
rm -f "$md"

for cmd in super jq; do
  if ! [[ $(type -P "$cmd") ]]; then
    echo "$cmd not found in PATH"
    exit 1
  fi
done

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
  '[.[] | select((.payload.pull_request.body // "") | contains("in case you have any feedback 😊"))] | length as $l | .[0] | .count = $l | {count}'
  '[.[] | select(to_entries | any(.value | tostring | contains("in case you have any feedback 😊")))] | length as $l | .[0] | .count = $l | {count}'
  '[.[] | select(.actor.login == "johnbieren")] | length as $l | .[0] | .count = $l | {count}'
  '.[] | [select(.repo.name == "duckdb/duckdb")] | group_by(.type)[] | length as $l | .[0] | .count = $l | {count,"type"}'
  '[.[] | (.payload.pull_request.assignee.login // empty, .payload.pull_request.assignees[]?.login // empty)] | group_by(.) | map({assignee: .[0], count: length}) | sort_by(.count) | reverse | .[:5] | .[]'
)

declare -a jq_flags=(
  '-c'
  '-c -s'
  '-c -s'
  '-c -s'
  '-c -s'
  '-c -s'
)

declare -a results=(
  ''
  '{"count":2}'
  '{"count":3}'
  '{"count":44}'
  '{"count":1,"type":"IssueCommentEvent"}{"count":1,"type":"WatchEvent"}'
  '{"assignee":"poad","count":172}{"assignee":"JazzarKarim","count":48}{"assignee":"vbudhram","count":32}{"assignee":"patrickangle","count":30}{"assignee":"johnbelamaric","count":29}'
)
for (( n=0; n<"${#superdb_queries[@]}"; n++ )); do
  desc=${descriptions[$n]}
  echo -e "### $desc\n" | tee -a "$md"
  echo "|**<br>Tool**|**<br>Arguments**|**Input<br>Format**|**Output<br>Format**|**<br>Real**|**<br>User**|**<br>Sys**|" | tee -a "$md"
  echo "|:----------:|:---------------:|:-----------------:|:------------------:|-----------:|-----------:|----------:|" | tee -a "$md"
  for input_format in json sup bsup csup; do
    for output_format in json sup bsup csup; do
      if [ $n -gt 0 ] && [ $output_format != "json" ]; then
        continue
      fi
      if [ $input_format = "json" ]; then
        input_file="${data}.json"
      else
        input_file="${data}.${input_format}"
      fi
      superdb_query=${superdb_queries[$n]/__SOURCE__/${data}.${input_format}}
      echo -n "|\`super\`|\`-c \"${superdb_query//|/\\|}\"\`|$input_format|$output_format|" | tee -a "$md"
      output_file=$(mktemp)
      all_times=$(mktemp)
      if [ $input_format = "csup" ]; then
        export SUPER_VAM=1
      else
        unset SUPER_VAM
      fi
      { time -p super -i $input_format -f $output_format -c "$superdb_query" "$input_file"; } > "$output_file" 2> "$all_times"
      < "$all_times" tr '\n' ' ' | awk '{ print $2 "|" $4 "|" $6 "|" }' | tee -a "$md"
      rm "$all_times"
      if [ $n -gt 0 ] && [ $output_format = "json" ] ; then
        diff <(jq < "$output_file" | sort) <(echo "${results[$n]}" | jq . | sort)
      elif [ $output_format != "json" ]; then
        mv "$output_file" "${data}.${output_format}"
      fi
    done
  done

  jq_query=${jq_filters[$n]}
  jq_flag=${jq_flags[$n]}
  echo -n "|\`jq\`|\`$jq_flag ""'""${jq_query//|/\\|}""'""\`|json|json|" | tee -a "$md"
  all_times=$(mktemp)
  # shellcheck disable=SC2086      # For expanding Jjq_flag
  { time -p jq $jq_flag "$jq_query" "${data}.json"; } > /dev/null 2> "$all_times"
  < "$all_times" tr '\n' ' ' | awk '{ print $2 "|" $4 "|" $6 "|" }' | tee -a "$md"

  echo | tee -a "$md"
done
