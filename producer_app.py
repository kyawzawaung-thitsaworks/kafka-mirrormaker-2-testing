"""Producer app: creates topics and generates numbered messages.

Messages go to the active cluster. When the health checker switches to the
backup, messages that were not confirmed by the primary are sent again to the
backup (at-least-once), so duplicates are possible but nothing the app knows
about is dropped.
"""
import json
import os
import threading
import time
import uuid
from collections import deque

from confluent_kafka import KafkaError, KafkaException, Producer
from flask import Flask, jsonify, render_template, request

import common as c
import webauth

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, "producer_state.json")
PORT = int(os.environ.get("PRODUCER_PORT", "8080"))

app = Flask(__name__, template_folder=os.path.join(HERE, "templates"), static_folder=os.path.join(HERE, "static"))
webauth.protect(app)
cm = c.ClusterManager("producer")


class Generator:
    def __init__(self):
        self.lock = threading.Lock()
        self.topic = None
        self.rate = 20
        self.running = False
        self.thread = None
        self.next_seq = self._load_state()
        self.producers = {}
        self.retry = deque()
        self.last_error = ""
        self.last_seq = 0
        self.stats = {"generated": 0, "delivered": 0, "delivered_primary": 0, "delivered_backup": 0,
                      "moved": 0, "failed": 0, "resent": 0}
        self.started_at = None

    # ---- state ---------------------------------------------------------
    def _load_state(self):
        try:
            with open(STATE_FILE) as f:
                return json.load(f).get("next_seq", {})
        except Exception:
            return {}

    def save_state(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump({"next_seq": self.next_seq}, f)
        except Exception:
            pass

    # ---- kafka ---------------------------------------------------------
    def producer(self, cluster):
        with self.lock:
            p = self.producers.get(cluster)
            if p is None:
                p = Producer(c.client_conf(cluster, **{
                    "client.id": "lab-ui-producer",
                    "acks": "all",
                    "linger.ms": 5,
                    # Long enough to survive until the health checker fails over.
                    "message.timeout.ms": 30000,
                }))
                self.producers[cluster] = p
            return p

    def on_delivery(self, err, msg, payload, cluster):
        with self.lock:
            if err is None:
                self.stats["delivered"] += 1
                self.stats[f"delivered_{cluster}"] += 1
                return
            if err.code() in (KafkaError._PURGE_QUEUE, KafkaError._PURGE_INFLIGHT):
                self.stats["moved"] += 1
            else:
                self.stats["failed"] += 1
                self.last_error = f"{cluster}: {err.str()}"
        self.retry.append(payload)  # sent again on the next loop

    def send(self, payload, cluster):
        payload["written_to"] = cluster
        p = self.producer(cluster)
        try:
            p.produce(
                payload["topic"],
                key=str(payload["seq"]),
                value=json.dumps(payload),
                on_delivery=lambda err, msg, pl=payload, cl=cluster: self.on_delivery(err, msg, pl, cl),
            )
            return True
        except BufferError:
            self.retry.appendleft(payload)
            p.poll(0.2)
        except KafkaException as exc:
            self.last_error = str(exc)
            self.retry.appendleft(payload)
            time.sleep(0.2)
        return False

    def on_switch(self, old, new):
        """Take messages the old cluster has not confirmed and queue them for
        the new cluster."""
        p = self.producers.get(old)
        if p is None:
            return
        waiting = len(p)
        p.purge(in_queue=True, in_flight=True, blocking=False)
        p.poll(1)
        if waiting:
            cm.log(f"Producer: {waiting} unconfirmed messages will be sent again to {new}", "warn")

    # ---- generator loop -----------------------------------------------
    def start(self, topic, rate):
        with self.lock:
            if self.running:
                return "already running"
            self.topic = topic
            self.rate = max(0.5, min(float(rate), 5000))
            self.running = True
            self.started_at = c.clock()
        self.thread = threading.Thread(target=self._run, name="generator", daemon=True)
        self.thread.start()
        cm.log(f"Producer: started writing to {topic} at {self.rate:g} messages/second")
        return "started"

    def stop(self):
        if not self.running:
            return "not running"
        self.running = False
        if self.thread:
            self.thread.join(timeout=10)
        cm.log("Producer: stopped")
        return "stopped"

    def _run(self):
        interval = 1.0 / self.rate
        next_t = time.monotonic()
        last_save = time.monotonic()
        while self.running:
            cluster = cm.active
            p = self.producer(cluster)
            # First send again anything that failed or was moved off the old cluster.
            n = 0
            while self.retry and n < 1000:
                payload = self.retry.popleft()
                if self.send(payload, cluster):
                    with self.lock:
                        self.stats["resent"] += 1
                n += 1
            seq = self.next_seq.get(self.topic, 1)
            self.next_seq[self.topic] = seq + 1
            payload = {"seq": seq, "id": uuid.uuid4().hex[:8], "created_ms": c.now_ms(), "topic": self.topic}
            self.send(payload, cluster)
            with self.lock:
                self.stats["generated"] += 1
                self.last_seq = seq
            p.poll(0)
            if time.monotonic() - last_save > 2:
                self.save_state()
                last_save = time.monotonic()
            next_t += interval
            delay = next_t - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            elif delay < -1:
                next_t = time.monotonic()
        try:
            self.producer(cm.active).flush(5)
        except Exception:
            pass
        self.save_state()

    def status(self):
        with self.lock:
            waiting = sum(len(p) for p in self.producers.values()) + len(self.retry)
            return {
                "running": self.running,
                "topic": self.topic,
                "rate": self.rate,
                "last_seq": self.last_seq,
                "stats": dict(self.stats),
                "waiting": waiting,
                "last_error": self.last_error,
                "started_at": self.started_at,
            }


class MirrorWatch:
    """Every 2 seconds: how many messages are in the topic on the primary,
    how many MirrorMaker has copied to the backup, and the difference (lag)."""

    def __init__(self):
        self.counters = {name: c.Counter(name) for name in c.CLUSTERS}
        self.topic = None
        self.data = {}
        self.last_primary = None

    def start(self):
        threading.Thread(target=self._loop, name="mirror-watch", daemon=True).start()

    def _loop(self):
        while True:
            topic = self.topic
            if topic:
                a = self.counters["primary"].total(topic)
                copied = self.counters["backup"].total(c.remote_topic(topic))
                warning = ""
                if c.SAME_NAMES:
                    # The copy and failover writes share one topic, so there is
                    # no separate "written to the backup" count.
                    local = None
                    if self.counters["backup"].total(c.prefixed_copy(topic)) is not None:
                        warning = (f"{c.prefixed_copy(topic)} exists on the backup. If MirrorMaker still adds the "
                                   f"'{c.PRIMARY_ALIAS}.' prefix, set REPLICATION_POLICY=default in lab.env, "
                                   f"or delete that old topic.")
                else:
                    local = self.counters["backup"].total(topic)
                if a is not None:
                    self.last_primary = (a, c.clock())
                self.data = {
                    "topic": topic,
                    "remote_topic": c.remote_topic(topic),
                    "same_names": c.SAME_NAMES,
                    "primary": a,
                    "primary_last": self.last_primary,
                    "copied": copied,
                    "backup_local": local,
                    "lag": (a - copied) if (a is not None and copied is not None) else None,
                    "warning": warning,
                    "time": c.clock(),
                }
            time.sleep(2)


gen = Generator()
watch = MirrorWatch()
cm.on_switch(gen.on_switch)


# ---- web ----------------------------------------------------------------
def valid_topic(name):
    return bool(name) and len(name) <= 200 and all(ch.isalnum() or ch in "._-" for ch in name)


@app.get("/")
def index():
    return render_template("producer.html")


@app.get("/api/status")
def api_status():
    return jsonify({"cluster": cm.status(), "generator": gen.status(), "mirror": watch.data})


@app.get("/api/topics")
def api_topics():
    try:
        topics = c.list_topics(cm.active)
        if not c.SAME_NAMES:
            topics = [t for t in topics if not t.startswith(c.PRIMARY_ALIAS + ".")]
        return jsonify({"topics": topics})
    except Exception as exc:
        return jsonify({"topics": [], "error": str(exc)})


@app.post("/api/topic")
def api_create_topic():
    body = request.get_json(force=True)
    name = (body.get("name") or "").strip()
    partitions = int(body.get("partitions") or 3)
    if not valid_topic(name):
        return jsonify({"ok": False, "message": "Use letters, numbers, dots, dashes or underscores."}), 400
    results = []
    # On the primary for normal use, and on the backup so producers have a
    # topic to write to after failover, even if MirrorMaker has not created
    # its copy yet. With same names, MirrorMaker then copies into that topic.
    for cluster in ("primary", "backup"):
        if cm.health.get(cluster) is False:
            results.append(f"skipped {cluster} (not reachable)")
        else:
            results.append(c.create_topic(cluster, name, partitions))
    cm.log("Create topic: " + "; ".join(results))
    watch.topic = watch.topic or name
    return jsonify({"ok": True, "message": "; ".join(results)})


@app.post("/api/start")
def api_start():
    body = request.get_json(force=True)
    topic = (body.get("topic") or "").strip()
    if not valid_topic(topic):
        return jsonify({"ok": False, "message": "Enter a topic name first."}), 400
    watch.topic = topic
    return jsonify({"ok": True, "message": gen.start(topic, body.get("rate") or 20)})


@app.post("/api/stop")
def api_stop():
    return jsonify({"ok": True, "message": gen.stop()})


@app.post("/api/watch")
def api_watch():
    topic = (request.get_json(force=True).get("topic") or "").strip()
    if valid_topic(topic):
        watch.topic = topic
    return jsonify({"ok": True})


@app.post("/api/switch")
def api_switch():
    to = request.get_json(force=True).get("to")
    if to not in c.CLUSTERS:
        return jsonify({"ok": False}), 400
    changed = cm.switch_to(to, "switched by hand")
    return jsonify({"ok": True, "message": f"now using {to}" if changed else f"already using {to}"})


@app.post("/api/auto-failover")
def api_auto_failover():
    cm.set_auto_failover(bool(request.get_json(force=True).get("enabled")))
    return jsonify({"ok": True})


if __name__ == "__main__":
    cm.start()
    watch.start()
    app.run(host=c.UI_HOST, port=PORT, threaded=True)
