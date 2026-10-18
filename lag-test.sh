#!/usr/bin/env bash
# Measure MirrorMaker lag. Examples:
#   ./lag-test.sh measure
#   ./lag-test.sh measure --count 20000 --rate 2000
#   ./lag-test.sh watch --topic orders
cd "$(dirname "$0")"
source ./lab.env
exec .venv/bin/python lag_test.py "$@"
