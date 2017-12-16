#!/usr/bin/env bash
set -e

SCRIPT_DIR=$(realpath $(dirname ${0}))

${SCRIPT_DIR}/generate.sh
psql ${@} -f $(dirname ${SCRIPT_DIR})/build/git.sql
