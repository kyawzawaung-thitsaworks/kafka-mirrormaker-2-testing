#!/usr/bin/env bash
# Encrypt the link between the two Kafkas (TLS) for the MirrorMaker lab.
# Run it on your own PC. It uses "ssh kafka" and "ssh kafka-backup" for the rest.
#
#   bash setup-tls.sh          turn on TLS
#   bash setup-tls.sh undo     go back to the plain port
#
# What "turn on TLS" does:
#   1. Your PC:      makes a certificate for the primary Kafka (kept in ~/kafka-certs)
#   2. kafka:        adds an encrypted port 9094 and restarts Kafka
#   3. kafka-backup: moves MirrorMaker to port 9094 and checks it still copies
#
# Needs bash, openssl, ssh and scp on your PC (Ubuntu and Mac have them).
# The plain port 9092 keeps working, so the apps are not affected.
# Firewall rules are NOT changed: port 9094 must be allowed from kafka-backup to kafka.
set -uo pipefail

SSH_PRIMARY="${SSH_PRIMARY:-kafka}"         # what you type after "ssh" for the primary
SSH_BACKUP="${SSH_BACKUP:-kafka-backup}"    # what you type after "ssh" for the backup
PRIMARY_VM="${PRIMARY_VM:-kafka}"           # the name the VMs use for the primary
BACKUP_VM="${BACKUP_VM:-kafka-backup}"
TLS_PORT="${TLS_PORT:-9094}"
CERT_DIR="$HOME/kafka-certs"
SCRIPT="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
REMOTE_SCRIPT="kafka-certs/$(basename "$0")"

# Used only on the VMs
KAFKA_HOME="$HOME/kafka"
CONFIG="$KAFKA_HOME/config/lab.properties"
MM2_CONFIG="$KAFKA_HOME/mm2.properties"
KAFKA_PATTERN='kafka\.Kafka .*lab\.properties'
MM2_PATTERN='connect\.mirror\.MirrorMaker'

step() { echo; echo "==> $*"; }
fail() { echo; echo "ERROR: $*"; exit 1; }

# ============================ on your PC =====================================

on() { ssh -n -o ConnectTimeout=10 "$@"; }

run_on() {  # ssh-name vm-step
  echo; echo "------------------------------ on $1 ------------------------------"
  on "$1" "PRIMARY_VM=$PRIMARY_VM BACKUP_VM=$BACKUP_VM TLS_PORT=$TLS_PORT bash ~/$REMOTE_SCRIPT $2"
}

check_vm() {  # ssh-name expected-hostname
  local name
  name=$(on "$1" hostname 2> /dev/null) || fail "Can't connect with: ssh $1   (Is the VPN on?)"
  if [ "$name" = "$2" ]; then
    echo "   ssh $1  ->  $name   OK"
  else
    echo "   ssh $1  ->  $name   (expected $2)"
    read -r -p "   Is this the right VM? (y/N) " answer
    [ "$answer" = "y" ] || { echo "Stopped. Nothing changed."; exit 1; }
  fi
}

check_apps() {  # restarting the primary makes the apps switch to the backup
  if on "$SSH_BACKUP" "pgrep -f '[p]roducer_app.py|[c]onsumer_app.py'" > /dev/null 2>&1; then
    echo
    echo "The producer/consumer apps are running on $SSH_BACKUP."
    echo "Restarting the primary Kafka will make them switch to the backup."
    echo "To avoid that, stop them first:  ./run.sh stop"
    read -r -p "Continue anyway? (y/N) " answer
    [ "$answer" = "y" ] || { echo "Stopped. Nothing changed."; exit 1; }
  fi
}

make_cert() {  # PRIMARY_IP must be set
  mkdir -p "$CERT_DIR" && chmod 700 "$CERT_DIR" && cd "$CERT_DIR" || exit 1
  [ -f keystore.pass ] || openssl rand -hex 16 > keystore.pass
  PASS=$(cat keystore.pass)

  local new_ca=0
  if [ -f ca.key ]; then
    step "Using the existing certificate authority (CA) in $CERT_DIR"
  else
    step "Creating a certificate authority (CA) in $CERT_DIR"
    openssl req -x509 -newkey rsa:2048 -nodes -days 825 -keyout ca.key -out ca.crt \
      -subj "/CN=kafka-lab-ca" 2> /dev/null || fail "Could not create the CA."
    new_ca=1
  fi

  if [ -f "$PRIMARY_VM.p12" ] && [ "$new_ca" = 0 ]; then
    step "Using the existing certificate for $PRIMARY_VM"
  else
    step "Creating a certificate for $PRIMARY_VM ($PRIMARY_IP)"
    openssl req -newkey rsa:2048 -nodes -keyout "$PRIMARY_VM.key" -out "$PRIMARY_VM.csr" \
      -subj "/CN=$PRIMARY_VM" 2> /dev/null || fail "Could not create the key."
    echo "subjectAltName=DNS:$PRIMARY_VM,IP:$PRIMARY_IP" > "$PRIMARY_VM.ext"
    openssl x509 -req -in "$PRIMARY_VM.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 \
      -out "$PRIMARY_VM.crt" -extfile "$PRIMARY_VM.ext" 2> /dev/null || fail "Could not sign the certificate."
    openssl pkcs12 -export -in "$PRIMARY_VM.crt" -inkey "$PRIMARY_VM.key" -certfile ca.crt \
      -name "$PRIMARY_VM" -out "$PRIMARY_VM.p12" -passout "pass:$PASS" || fail "Could not create the keystore."
  fi
  chmod 600 ./*.key ./*.p12 keystore.pass
  echo "   Valid for: $(openssl x509 -in "$PRIMARY_VM.crt" -noout -text | grep -A1 'Subject Alternative Name' | tail -1 | sed 's/^ *//')"
  echo "   Expires:   $(openssl x509 -in "$PRIMARY_VM.crt" -noout -enddate | cut -d= -f2)"
}

do_setup() {
  for tool in openssl ssh scp; do
    command -v "$tool" > /dev/null || fail "'$tool' is missing on this PC."
  done

  step "Checking the SSH connections"
  check_vm "$SSH_PRIMARY" "$PRIMARY_VM"
  check_vm "$SSH_BACKUP" "$BACKUP_VM"
  PRIMARY_IP=$(on "$SSH_PRIMARY" "hostname -I" | awk '{print $1}')
  [ -n "$PRIMARY_IP" ] || fail "Could not read the IP address of $SSH_PRIMARY."
  check_apps

  make_cert

  step "Copying the files to the VMs"
  on "$SSH_PRIMARY" "mkdir -p ~/kafka-certs && chmod 700 ~/kafka-certs" || fail "Could not create ~/kafka-certs on $SSH_PRIMARY."
  on "$SSH_BACKUP"  "mkdir -p ~/kafka-certs && chmod 700 ~/kafka-certs" || fail "Could not create ~/kafka-certs on $SSH_BACKUP."
  scp -q "$PRIMARY_VM.p12" keystore.pass ca.crt "$SCRIPT" "$SSH_PRIMARY:kafka-certs/" || fail "Could not copy to $SSH_PRIMARY."
  scp -q ca.crt "$SCRIPT" "$SSH_BACKUP:kafka-certs/" || fail "Could not copy to $SSH_BACKUP."
  echo "   $SSH_PRIMARY: $PRIMARY_VM.p12, keystore.pass, ca.crt, $(basename "$SCRIPT")"
  echo "   $SSH_BACKUP:  ca.crt, $(basename "$SCRIPT")"

  run_on "$SSH_PRIMARY" vm-primary || fail "The step on $SSH_PRIMARY failed (see above). To undo: bash $0 undo"
  run_on "$SSH_BACKUP"  vm-backup  || fail "The step on $SSH_BACKUP failed (see above). To undo: bash $0 undo"

  echo; echo "------------------------------------------------------------------"
  echo "All done. MirrorMaker now reads from the primary over TLS (port $TLS_PORT)."
  echo "Your apps still use the plain port 9092. Keep $CERT_DIR/ca.key on this PC only."
}

do_undo() {
  step "Checking the SSH connections"
  check_vm "$SSH_PRIMARY" "$PRIMARY_VM"
  check_vm "$SSH_BACKUP" "$BACKUP_VM"
  check_apps
  run_on "$SSH_BACKUP"  vm-undo-backup  || fail "The undo on $SSH_BACKUP failed (see above)."
  run_on "$SSH_PRIMARY" vm-undo-primary || fail "The undo on $SSH_PRIMARY failed (see above)."
  echo; echo "------------------------------------------------------------------"
  echo "Undo finished. Both VMs are back to the plain port 9092 only."
}

# ============================ on the VMs =====================================

wait_for() {  # seconds function-name : true when the function succeeds in time
  local deadline=$((SECONDS + $1)) fn=$2
  while [ "$SECONDS" -lt "$deadline" ]; do
    "$fn" && return 0
    sleep 1
  done
  return 1
}
kafka_running() { pgrep -f "$KAFKA_PATTERN" > /dev/null; }
kafka_stopped() { ! kafka_running; }
mm2_running()   { pgrep -f "$MM2_PATTERN" > /dev/null; }
mm2_stopped()   { ! mm2_running; }
port_open()     { timeout 2 bash -c "exec 3<>/dev/tcp/127.0.0.1/$WAIT_PORT" 2> /dev/null; }

restart_kafka() {  # port to wait for
  step "Restarting Kafka"
  if kafka_running; then
    "$KAFKA_HOME/bin/kafka-server-stop.sh" > /dev/null 2>&1
    wait_for 40 kafka_stopped || { echo "   It didn't stop in 40 s, forcing it."; pkill -9 -f "$KAFKA_PATTERN"; sleep 2; }
  fi
  "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$CONFIG"
  WAIT_PORT=$1
  if wait_for 60 port_open; then
    echo "   Kafka is running, and port $1 is open."
    return 0
  fi
  echo
  echo "ERROR: port $1 didn't open within 60 s. Last errors from the Kafka log:"
  grep -E "ERROR|FATAL" "$KAFKA_HOME/logs/server.log" 2> /dev/null | tail -5
  return 1
}

restart_mm2() {
  step "Restarting MirrorMaker"
  if mm2_running; then
    pkill -f "$MM2_PATTERN"
    wait_for 30 mm2_stopped || { pkill -9 -f "$MM2_PATTERN"; sleep 2; }
  fi
  nohup "$KAFKA_HOME/bin/connect-mirror-maker.sh" "$MM2_CONFIG" > "$KAFKA_HOME/mm2.log" 2>&1 < /dev/null &
  wait_for 30 mm2_running || fail "MirrorMaker didn't start. Check: tail -30 $KAFKA_HOME/mm2.log"
  echo "   Started."
}

heartbeats() {  # messages in primary.heartbeats on the backup (MirrorMaker copies them every second)
  "$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$BACKUP_VM:9092" --topic primary.heartbeats 2> /dev/null \
    | awk -F: '{s+=$3} END {print s+0}'
}
HB_START=0
heartbeats_growing() { [ "$(heartbeats)" -gt "$HB_START" ]; }

vm_primary() {
  [ -f "$CONFIG" ] || fail "Can't find $CONFIG."
  PASS=$(cat "$HOME/kafka-certs/keystore.pass")

  if grep -q "SSL://:$TLS_PORT" "$CONFIG"; then
    step "lab.properties already has the encrypted port $TLS_PORT. Not changing it."
  else
    step "Adding the encrypted port $TLS_PORT to lab.properties"
    [ -f "$CONFIG.before-tls" ] || cp "$CONFIG" "$CONFIG.before-tls"
    sed -i "/^listeners=/ s#\$#,SSL://:$TLS_PORT#" "$CONFIG"
    sed -i "/^advertised.listeners=/ s#\$#,SSL://$PRIMARY_VM:$TLS_PORT#" "$CONFIG"
    sed -i "/^listener.security.protocol.map=/ s#\$#,SSL:SSL#" "$CONFIG"
    cat >> "$CONFIG" <<EOF

# Encrypted port $TLS_PORT (TLS), added by setup-tls.sh
ssl.keystore.type=PKCS12
ssl.keystore.location=$HOME/kafka-certs/$PRIMARY_VM.p12
ssl.keystore.password=$PASS
ssl.key.password=$PASS
EOF
    chmod 600 "$CONFIG"
    echo "   Backup of the old settings: $CONFIG.before-tls"
  fi
  grep -E '^(listeners|advertised.listeners|listener.security.protocol.map)=' "$CONFIG" | sed 's/^/   /'

  restart_kafka "$TLS_PORT" || exit 1

  step "Checking the certificate on port $TLS_PORT"
  if echo | timeout 15 openssl s_client -connect "localhost:$TLS_PORT" -CAfile "$HOME/kafka-certs/ca.crt" \
       -verify_hostname "$PRIMARY_VM" 2> /dev/null | grep -q "Verify return code: 0"; then
    echo "   OK: the certificate is valid for $PRIMARY_VM."
  else
    fail "The certificate check failed."
  fi
}

vm_backup() {
  [ -f "$MM2_CONFIG" ] || fail "Can't find $MM2_CONFIG."
  CA="$HOME/kafka-certs/ca.crt"

  step "Checking the encrypted connection to $PRIMARY_VM:$TLS_PORT"
  OUT=$(echo | timeout 15 openssl s_client -connect "$PRIMARY_VM:$TLS_PORT" -CAfile "$CA" \
          -verify_hostname "$PRIMARY_VM" 2>&1)
  if echo "$OUT" | grep -q "Verify return code: 0"; then
    echo "   OK: connected, and the certificate is valid for $PRIMARY_VM."
  elif echo "$OUT" | grep -q "Verify return code"; then
    echo "$OUT" | grep "Verify return code" | sed 's/^/   /'
    fail "The certificate check failed."
  else
    fail "Can't connect to $PRIMARY_VM:$TLS_PORT. Is port $TLS_PORT allowed from $BACKUP_VM to $PRIMARY_VM?"
  fi

  CLIENT_PROPS="$HOME/kafka-certs/tls-client.properties"
  printf 'security.protocol=SSL\nssl.truststore.type=PEM\nssl.truststore.location=%s\n' "$CA" > "$CLIENT_PROPS"
  step "Checking that Kafka answers over the encrypted port"
  TOPICS=$(timeout 60 "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$PRIMARY_VM:$TLS_PORT" \
             --command-config "$CLIENT_PROPS" --list 2> /dev/null) \
    || fail "Kafka didn't answer on $PRIMARY_VM:$TLS_PORT."
  echo "   OK: the primary answered and lists $(echo "$TOPICS" | grep -c .) topics."

  if grep -q "^primary.security.protocol" "$MM2_CONFIG"; then
    step "mm2.properties already uses TLS for the primary. Not changing it."
  else
    step "Moving MirrorMaker to the encrypted port"
    [ -f "$MM2_CONFIG.before-tls" ] || cp "$MM2_CONFIG" "$MM2_CONFIG.before-tls"
    sed -i "s#^primary.bootstrap.servers.*#primary.bootstrap.servers = $PRIMARY_VM:$TLS_PORT#" "$MM2_CONFIG"
    cat >> "$MM2_CONFIG" <<EOF

# Connect to the primary over TLS, added by setup-tls.sh
primary.security.protocol = SSL
primary.ssl.truststore.type = PEM
primary.ssl.truststore.location = $CA
EOF
    echo "   Backup of the old settings: $MM2_CONFIG.before-tls"
  fi
  grep -E '^primary\.(bootstrap|security|ssl)' "$MM2_CONFIG" | sed 's/^/   /'

  HB_START=$(heartbeats)
  restart_mm2

  step "Checking that MirrorMaker copies over the encrypted connection (up to 2 minutes)"
  if wait_for 120 heartbeats_growing; then
    echo "   OK: new heartbeats from the primary are arriving on the backup."
  else
    echo "   No new heartbeats. Recent MirrorMaker errors:"
    grep -iE "ERROR|SSL" "$KAFKA_HOME/mm2.log" | tail -5 | sed 's/^/   /'
    exit 1
  fi
}

vm_undo_primary() {
  if [ ! -f "$CONFIG.before-tls" ]; then
    step "No TLS changes found in lab.properties. Nothing to undo here."
    return 0
  fi
  step "Restoring lab.properties from before TLS"
  mv "$CONFIG.before-tls" "$CONFIG"
  restart_kafka 9092 || exit 1
}

vm_undo_backup() {
  if [ ! -f "$MM2_CONFIG.before-tls" ]; then
    step "No TLS changes found in mm2.properties. Nothing to undo here."
    return 0
  fi
  step "Restoring mm2.properties from before TLS"
  mv "$MM2_CONFIG.before-tls" "$MM2_CONFIG"
  restart_mm2
}

case "${1:-setup}" in
  setup)           do_setup ;;
  undo)            do_undo ;;
  vm-primary)      vm_primary ;;
  vm-backup)       vm_backup ;;
  vm-undo-primary) vm_undo_primary ;;
  vm-undo-backup)  vm_undo_backup ;;
  *) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac