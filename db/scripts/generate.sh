#!/usr/bin/env bash
set -e

cd $(dirname ${0})/..

OUT="build/git.sql"

mkdir -p $(dirname "${OUT}")

if [ -f "${OUT}" ]
then
  rm "${OUT}"
fi

echo "-- Git SQL --" >> ${OUT}
function sql() {
  for F in ${@}
  do
    echo "[Use] ${F}"
    echo "" >> "${OUT}"
    echo "-- File: ${F} --" >> "${OUT}"
    echo "" >> "${OUT}"
    cat ${F} >> "${OUT}"
  done
}

sql headers/*.sql
sql tables/*.sql
sql views/*.sql
sql functions/*.sql
