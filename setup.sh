#!/usr/bin/env bash
# One-time setup for the lab. Run it on each VM, primary first:
#
#   ./setup.sh primary   on kafka:        Java and Kafka
#   ./setup.sh backup    on kafka-backup: Java, Kafka, MirrorMaker and the apps
#   ./setup.sh apps      on kafka-backup: only the apps (Python environment and libraries)
#
# Without a word, it picks primary or backup from the VM's name.
# Safe to run again: whatever is already installed, configured or running is kept.
# Change the defaults like this:  KAFKA_VERSION=4.1.0 ./setup.sh primary
set -euo pipefail
cd "$(dirname "$0")"

PRIMARY_VM="${PRIMARY_VM:-kafka}"           # the name the VMs use for the primary
BACKUP_VM="${BACKUP_VM:-kafka-backup}"
KAFKA_VERSION="${KAFKA_VERSION:-4.1.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_HOME="$HOME/kafka"
DATA_DIR="$HOME/kafka-data"
CONFIG="$KAFKA_HOME/config/lab.properties"
MM2_CONFIG="$KAFKA_HOME/mm2.properties"
KAFKA_PATTERN='kafka\.Kafka .*lab\.properties'
MM2_PATTERN='connect\.mirror\.MirrorMaker'

step() { echo; echo "==> $*"; }
fail() { echo; echo "ERROR: $*"; exit 1; }

wait_for() {  # seconds command... : true when the command succeeds in time
  local deadline=$((SECONDS + $1)); shift
  while [ "$SECONDS" -lt "$deadline" ]; do
    "$@" && return 0
    sleep 1
  done
  return 1
}
port_open() { timeout 2 bash -c "exec 3<>/dev/tcp/$1/$2" 2> /dev/null; }  # host port

# ============================ Java and Kafka ================================

install_java() {
  local major
  major=$(java -version 2>&1 | awk -F'"' '/version/ {split($2, v, "."); print v[1]}' || true)
  if [ -n "$major" ] && [ "$major" -ge 17 ]; then
    step "Java $major is already installed"
    return
  fi
  step "Installing Java (needs sudo)"
  sudo apt-get update -qq
  sudo apt-get install -y -qq openjdk-21-jre-headless 2> /dev/null \
    || sudo apt-get install -y -qq openjdk-17-jre-headless \
    || fail "Could not install Java 17 or 21."
  java -version 2>&1 | head -1 | sed 's/^/   /'
}

install_kafka() {
  if [ -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
    step "Kafka is already installed in $KAFKA_HOME"
    return
  fi
  local file="kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz"
  step "Downloading Kafka $KAFKA_VERSION"
  curl -fsSL -o "/tmp/$file" "https://downloads.apache.org/kafka/$KAFKA_VERSION/$file" \
    || curl -fsSL -o "/tmp/$file" "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$file" \
    || fail "Could not download $file. Check KAFKA_VERSION."
  mkdir -p "$KAFKA_HOME"
  tar -xzf "/tmp/$file" -C "$KAFKA_HOME" --strip-components=1
  rm -f "/tmp/$file"
  echo "   Installed in $KAFKA_HOME"
}

write_kafka_config() {  # this VM's name
  if [ -f "$CONFIG" ]; then
    step "$CONFIG already exists. Not changing it."
    return
  fi
  step "Writing $CONFIG"
  cat > "$CONFIG" <<EOF
# Single-node Kafka for the DR lab (broker and controller in one process), written by setup.sh
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=$1:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://$1:9092,CONTROLLER://$1:9093
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=$DATA_DIR
num.partitions=3

# Only one broker, so every internal topic has one copy
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
}

format_storage() {
  if [ -f "$DATA_DIR/meta.properties" ]; then
    step "$DATA_DIR is already formatted"
    return
  fi
  step "Formatting $DATA_DIR"
  "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone \
    -t "$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)" -c "$CONFIG" | sed 's/^/   /'
}

start_kafka() {
  if pgrep -f "$KAFKA_PATTERN" > /dev/null; then
    step "Kafka is already running"
    return
  fi
  step "Starting Kafka"
  "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$CONFIG"
  if wait_for 60 port_open 127.0.0.1 9092; then
    echo "   Kafka is running on port 9092."
  else
    echo "   Port 9092 didn't open within 60 s. Last errors from the Kafka log:"
    grep -E "ERROR|FATAL" "$KAFKA_HOME/logs/server.log" 2> /dev/null | tail -5 | sed 's/^/   /'
    exit 1
  fi
}

setup_kafka() {  # this VM's name
  install_java
  install_kafka
  write_kafka_config "$1"
  format_storage
  start_kafka
}

# ============================ MirrorMaker ===================================

write_mm2_config() {
  if [ -f "$MM2_CONFIG" ]; then
    step "$MM2_CONFIG already exists. Not changing it."
    return
  fi
  step "Writing $MM2_CONFIG"
  cat > "$MM2_CONFIG" <<EOF
# MirrorMaker 2 for the DR lab: copies the primary into the backup, written by setup.sh
clusters = primary, backup
primary.bootstrap.servers = $PRIMARY_VM:9092
backup.bootstrap.servers = $BACKUP_VM:9092

# One direction only. With same topic names, copying both ways would loop forever.
primary->backup.enabled = true
primary->backup.topics = .*
primary->backup.groups = .*
backup->primary.enabled = false

# Same topic names on both clusters (orders -> orders). Must match REPLICATION_POLICY in lab.env.
replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy

# Copy the consumers' bookmarks too, so they continue on the backup after a failover
sync.group.offsets.enabled = true
sync.group.offsets.interval.seconds = 5
emit.checkpoints.interval.seconds = 5
refresh.topics.interval.seconds = 10
refresh.groups.interval.seconds = 10

# Only one broker per cluster, so every topic has one copy
replication.factor = 1
checkpoints.topic.replication.factor = 1
heartbeats.topic.replication.factor = 1
offset-syncs.topic.replication.factor = 1
offset.storage.replication.factor = 1
status.storage.replication.factor = 1
config.storage.replication.factor = 1
EOF
}

start_mm2() {
  if pgrep -f "$MM2_PATTERN" > /dev/null; then
    step "MirrorMaker is already running"
    return
  fi
  step "Checking that the primary ($PRIMARY_VM:9092) can be reached"
  if wait_for 30 port_open "$PRIMARY_VM" 9092; then
    echo "   OK."
  else
    echo "   Can't reach $PRIMARY_VM:9092. Run ./setup.sh primary on $PRIMARY_VM first, and check"
    echo "   that port 9092 is allowed from $BACKUP_VM. MirrorMaker will keep retrying."
  fi
  step "Starting MirrorMaker"
  nohup "$KAFKA_HOME/bin/connect-mirror-maker.sh" "$MM2_CONFIG" > "$KAFKA_HOME/mm2.log" 2>&1 < /dev/null &
  wait_for 30 pgrep -f "$MM2_PATTERN" > /dev/null || fail "MirrorMaker didn't start. Check: tail -30 $KAFKA_HOME/mm2.log"
  echo "   Started. Log: $KAFKA_HOME/mm2.log"
}

# ============================ the apps ======================================

setup_apps() {
  step "Installing python3-venv (needs sudo)"
  sudo apt-get update -qq
  sudo apt-get install -y -qq python3-venv

  step "Creating the Python environment in .venv"
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -r requirements.txt

  chmod +x run.sh slow-link.sh lag-test.sh
}

# ============================================================================

ROLE="${1:-}"
if [ -z "$ROLE" ]; then
  case "$(hostname)" in
    "$PRIMARY_VM") ROLE=primary ;;
    "$BACKUP_VM")  ROLE=backup ;;
    *) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
  esac
  echo "This VM is $(hostname), so: ./setup.sh $ROLE"
fi

case "$ROLE" in
  primary)
    setup_kafka "$PRIMARY_VM"
    echo
    echo "Setup done. Next: ./setup.sh backup on $BACKUP_VM"
    ;;
  backup)
    setup_kafka "$BACKUP_VM"
    write_mm2_config
    start_mm2
    setup_apps
    echo
    echo "Setup done. Next: ./run.sh start"
    ;;
  apps)
    setup_apps
    echo
    echo "Setup done. Next: ./run.sh start"
    ;;
  *) sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
