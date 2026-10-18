#!/usr/bin/env bash
# Simulate a slow network link between the sites.
#
# Limits how fast consumers can read from the PRIMARY Kafka. MirrorMaker reads
# the primary as a consumer, so it copies more slowly and lag builds up.
# Producers are not affected.
#
#   ./slow-link.sh on 1      # limit reading to 1 MB per second (decimals ok: 0.5)
#   ./slow-link.sh off       # remove the limit
#   ./slow-link.sh status
#
# Note: the limit applies to every consumer of the primary, including the
# consumer app while it reads from the primary.
set -euo pipefail
cd "$(dirname "$0")"
source ./lab.env

CONFIGS="$KAFKA_HOME/bin/kafka-configs.sh"
[ -x "$CONFIGS" ] || { echo "Cannot find $CONFIGS. Set KAFKA_HOME in lab.env."; exit 1; }
BASE=(--bootstrap-server "$PRIMARY_BOOTSTRAP" --entity-type clients --entity-default)

case "${1:-status}" in
  on)
    MB="${2:-1}"
    BYTES=$(awk -v mb="$MB" 'BEGIN { printf "%d", mb * 1048576 }')
    "$CONFIGS" "${BASE[@]}" --alter --add-config "consumer_byte_rate=$BYTES" > /dev/null
    echo "Slow link ON: reading from the primary is limited to $MB MB per second."
    ;;
  off)
    "$CONFIGS" "${BASE[@]}" --alter --delete-config consumer_byte_rate > /dev/null 2>&1 || true
    echo "Slow link OFF: no limit."
    ;;
  status)
    OUT=$("$CONFIGS" "${BASE[@]}" --describe 2>/dev/null || true)
    RATE=$(echo "$OUT" | grep -o 'consumer_byte_rate=[0-9.]*' | cut -d= -f2 | cut -d. -f1 || true)
    if [ -n "$RATE" ]; then
      echo "Slow link ON: $(awk -v b="$RATE" 'BEGIN { printf "%.2f", b / 1048576 }') MB per second."
    else
      echo "Slow link OFF."
    fi
    ;;
  *)
    echo "Usage: $0 on [MB_per_second] | off | status"
    exit 1
    ;;
esac
