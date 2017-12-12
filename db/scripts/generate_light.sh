#!/usr/bin/env bash
set -e

cd "$(dirname ${0})/.."

OUT="build/git-light.sql"

mkdir -p "$(dirname "${OUT}")"

if [ -f "${OUT}" ]
then
  rm "${OUT}"
fi

echo "-- Git SQL --" >> "${OUT}"
function sql() {
  for F in "${@}"
  do
    echo "[Include] ${F}"
    {
      echo ""
      echo "-- File ${F} --"
      echo ""
      cat "${F}"
    } >> "${OUT}"
  done
}

sql headers/*.sql
sql types/*.sql
sql tables/*refs.sql
sql functions/specials/*.sql
sql indexes/*.sql
sql views/*.sql
sql functions/*.sql

