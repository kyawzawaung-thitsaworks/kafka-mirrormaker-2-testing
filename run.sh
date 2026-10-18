#!/usr/bin/env bash
# Start, stop and check the producer and consumer apps.
#   ./run.sh start | stop | restart | status | logs
cd "$(dirname "$0")"
source ./lab.env
PY=.venv/bin/python

start_one() {  # name
  local name=$1
  if [ -f "$name.pid" ] && kill -0 "$(cat "$name.pid")" 2>/dev/null; then
    echo "$name app is already running (pid $(cat "$name.pid"))"
    return
  fi
  nohup "$PY" "${name}_app.py" >> "$name.log" 2>&1 &
  echo $! > "$name.pid"
  echo "$name app started (pid $!)"
}

stop_one() {  # name
  local name=$1
  if [ -f "$name.pid" ] && kill -0 "$(cat "$name.pid")" 2>/dev/null; then
    kill "$(cat "$name.pid")"
    echo "$name app stopped"
  else
    echo "$name app is not running"
  fi
  rm -f "$name.pid"
}

status_one() {  # name port
  local name=$1 port=$2
  if [ -f "$name.pid" ] && kill -0 "$(cat "$name.pid")" 2>/dev/null; then
    echo "$name app: running (pid $(cat "$name.pid"))"
    urls "$port"
  else
    echo "$name app: not running"
  fi
}

public_ip() {
  # Public IP from the Google Cloud metadata server. Empty when the VM has
  # only a private IP (or when not running on Google Cloud).
  curl -sf -m 2 -H "Metadata-Flavor: Google" \
    http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip 2>/dev/null
}

urls() {  # port
  local port=$1
  if [ "$UI_HOST" = "127.0.0.1" ] || [ "$UI_HOST" = "localhost" ]; then
    echo "    http://localhost:$port   (UI_HOST is 127.0.0.1: only from this VM)"
    return
  fi
  local private public
  private=$(hostname -I 2>/dev/null | awk '{print $1}')
  public=$(public_ip)
  [ -n "$private" ] && echo "    http://$private:$port   (private IP: open it over the VPN)"
  [ -n "$public" ] && echo "    http://$public:$port   (public IP)"
}

warn_open() {
  # Only matters if the VM can be reached from the internet.
  if [ "$UI_HOST" != "127.0.0.1" ] && [ "$UI_HOST" != "localhost" ] && [ -z "$UI_PASSWORD" ] && [ -n "$(public_ip)" ]; then
    echo
    echo "WARNING: this VM has a public IP and the UIs have no password."
    echo "         Set UI_PASSWORD in lab.env and run ./run.sh restart, or limit the firewall rule."
  fi
}

case "${1:-status}" in
  start)
    [ -x "$PY" ] || { echo "Run ./setup.sh first."; exit 1; }
    start_one producer
    start_one consumer
    sleep 2
    status_one producer "$PRODUCER_PORT"
    status_one consumer "$CONSUMER_PORT"
    warn_open
    ;;
  stop)
    stop_one producer
    stop_one consumer
    ;;
  restart)
    "$0" stop
    sleep 2
    "$0" start
    ;;
  status)
    status_one producer "$PRODUCER_PORT"
    status_one consumer "$CONSUMER_PORT"
    warn_open
    ;;
  logs)
    tail -n 30 -f producer.log consumer.log
    ;;
  *)
    echo "Usage: $0 start|stop|restart|status|logs"
    exit 1
    ;;
esac
