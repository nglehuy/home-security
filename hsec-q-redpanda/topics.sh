#!/bin/sh
# Creates the Redpanda topics and sets their retention.
# A second run changes nothing, except new retention values.
#
# BROKERS: the Kafka address, for example
# redpanda-0.redpanda.hsec.svc.cluster.local:9093
set -eu

: "${BROKERS:?set BROKERS to the Kafka address}"

WEEK_MS=604800000
MIB=1048576
GIB=1073741824

rpk_() {
  rpk -X brokers="$BROKERS" "$@"
}

# Wait up to 5 minutes for the broker.
tries=0
until rpk_ cluster info >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -ge 60 ]; then
    echo "Broker $BROKERS is not reachable." >&2
    exit 1
  fi
  sleep 5
done

# topic NAME RETENTION_MS RETENTION_BYTES [MAX_MESSAGE_BYTES]
# Each topic has 1 partition and 1 replica, so all messages of a camera stay in order.
topic() {
  name=$1
  ms=$2
  bytes=$3
  max=${4:-}
  if ! rpk_ topic describe "$name" >/dev/null 2>&1; then
    rpk_ topic create "$name" -p 1 -r 1
  fi
  rpk_ topic alter-config "$name" --set retention.ms="$ms" --set retention.bytes="$bytes"
  if [ -n "$max" ]; then
    rpk_ topic alter-config "$name" --set max.message.bytes="$max"
  fi
}

# Camera topics from the bridge.
topic frigate.events "$WEEK_MS" "$GIB"
topic frigate.reviews "$WEEK_MS" "$GIB"
topic frigate.tracked_object_update "$WEEK_MS" "$GIB"
topic frigate.triggers "$WEEK_MS" "$GIB"

# System topics from the bridge.
for name in frigate.available frigate.restart frigate.stats frigate.camera_activity \
  frigate.profile.set frigate.profile.state frigate.notifications.set frigate.notifications.state; do
  topic "$name" "$WEEK_MS" $((128 * MIB))
done

# Alerts from Flink. One message can carry a clip part of up to 19 MiB in base64.
topic hsec.alerts "$WEEK_MS" "$GIB" $((32 * MIB))

# Camera messages without a camera or a time.
topic hsec.rejected "$WEEK_MS" $((256 * MIB))
