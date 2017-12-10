#!/usr/bin/env bash
set -e

cd "$(dirname ${0})/.."

OUT="build/git.sql"

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
sql tables/*.sql
sql indexes/*.sql
sql views/*.sql
sql functions/*.sql
