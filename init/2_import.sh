#!/bin/bash
set -euo pipefail
until clickhouse-client --query="SELECT 1"; do sleep 1; done
shopt -s extglob nullglob
files=(/csv/MOCK_DATA\ \([5-9]\).csv)

if [ ${#files[@]} -eq 0 ]; then
  echo "Нет CSV-файлов 5–9" >&2
  exit 1
fi

for f in "${files[@]}"; do
  echo "Importing $f…"
  perl -pe '
    s{\b(\d{1,2})/(\d{1,2})/(\d{4})\b}{
      sprintf("%04d-%02d-%02d 00:00:00",$3,$1,$2)
    }gex
  ' < "$f" \
    | clickhouse-client \
        --query="INSERT INTO mydb.sales FORMAT CSVWithNames"
done

echo "Done."
