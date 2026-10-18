"""Consumer app: shows messages in real time and checks them.

Because the producer numbers every message, the consumer can tell:
- duplicates (a number seen twice)
- missing messages (a number never arrived)
- late arrivals (a missing number that turned up later)

On the primary it reads <topic>. After failover it continues on the backup
from the bookmark MirrorMaker copied. With same topic names it reads <topic>
there too; with default naming it reads both <alias>.<topic> (the copy) and
<topic> (written to the backup after failover).
"""
import bisect
import json
import os
import queue
import threading
import time
from collections import deque

from confluent_kafka import Consumer, KafkaError
from flask import Flask, Response, jsonify, render_template, request

import common as c
import webauth

HERE = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("CONSUMER_PORT", "8081"))
# A gap only counts as "missing" after this long. Messages from different
# partitions arrive slightly out of order, which causes short gaps.
GRACE_SEC = float(os.environ.get("MISSING_GRACE_SEC", "5"))

app = Flask(__name__, template_folder=os.path.join(HERE, "templates"), static_folder=os.path.join(HERE, "static"))
webauth.protect(app)
cm = c.ClusterManager("consumer")


class Checker:
    """Tracks message numbers: duplicates, gaps and late arrivals."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.seen = set()
        self.min_seq = None
        self.max_seq = 0
        self.g_start, self.g_end, self.g_time = [], [], []  # sorted gaps
        self.received = 0
        self.duplicates = 0
        self.late = 0
        self.latencies = deque(maxlen=200)

    def check(self, seq, now):
        self.received += 1
        if seq in self.seen:
            self.duplicates += 1
            return "duplicate"
        self.seen.add(seq)
        if self.min_seq is None:
            self.min_seq = self.max_seq = seq
            return "ok"
        if seq > self.max_seq:
            if seq > self.max_seq + 1:
                self.g_start.append(self.max_seq + 1)
                self.g_end.append(seq - 1)
                self.g_time.append(now)
            self.max_seq = seq
            return "ok"
        if seq < self.min_seq:
            # Lower than anything seen so far (other partitions or topics can
            # be read in a different order). Track the hole above it as a gap.
            if seq < self.min_seq - 1:
                self.g_start.insert(0, seq + 1)
                self.g_end.insert(0, self.min_seq - 1)
                self.g_time.insert(0, now)
            self.min_seq = seq
            return "ok"
        # Inside the range and not seen before: it fills a gap.
        i = bisect.bisect_right(self.g_start, seq) - 1
        if i >= 0 and self.g_end[i] >= seq:
            a, b, t = self.g_start[i], self.g_end[i], self.g_time[i]
            pieces = []
            if a <= seq - 1:
                pieces.append((a, seq - 1, t))
            if seq + 1 <= b:
                pieces.append((seq + 1, b, t))
            del self.g_start[i], self.g_end[i], self.g_time[i]
            for j, (pa, pb, pt) in enumerate(pieces):
                self.g_start.insert(i + j, pa)
                self.g_end.insert(i + j, pb)
                self.g_time.insert(i + j, pt)
            if now - t >= GRACE_SEC:
                self.late += 1
                return "late"
        return "ok"

    def missing(self, now):
        total, ranges = 0, []
        for a, b, t in zip(self.g_start, self.g_end, self.g_time):
            if now - t >= GRACE_SEC:
                total += b - a + 1
                ranges.append([a, b])
        return total, ranges


class Reader:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.thread = None
        self.topic = None
        self.group = "lab-ui-consumer"
        self.reading = None
        self.last_error = ""
        self.checker = Checker()
        self.clients = []
        self.retired = []

    def start(self, topic, group):
        if self.running:
            return "already reading"
        self.topic, self.group = topic, group
        self.running = True
        self.thread = threading.Thread(target=self._run, name="reader", daemon=True)
        self.thread.start()
        return "started"

    def stop(self):
        if not self.running:
            return "not reading"
        self.running = False
        if self.thread:
            self.thread.join(timeout=15)
        self.reading = None
        cm.log("Consumer: stopped")
        return "stopped"

    def _new_consumer(self, cluster):
        consumer = Consumer(c.client_conf(cluster, **{
            "group.id": self.group,
            "client.id": "lab-ui-consumer",
            # No bookmark for a partition? Start from the beginning, never skip.
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
            "session.timeout.ms": 10000,
            "topic.metadata.refresh.interval.ms": 5000,
        }))
        topics = c.read_topics(self.topic, cluster)
        consumer.subscribe(topics)
        self.reading = {"cluster": cluster, "topics": topics, "group": self.group, "since": c.clock()}
        cm.log(f"Consumer: reading {' and '.join(topics)} on {cluster}, group {self.group}",
               "alert" if cluster == "backup" else "info")
        return consumer

    def _retire(self, consumer):
        # Closing a consumer whose cluster is down can take a long time, so do
        # it in the background.
        self.retired.append(consumer)
        threading.Thread(target=consumer.close, daemon=True).start()

    def _run(self):
        consumer, cluster = None, None
        while self.running:
            want = cm.active
            if consumer is None or want != cluster:
                if consumer is not None:
                    self._retire(consumer)
                cluster = want
                consumer = self._new_consumer(cluster)
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    continue
                if code in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC):
                    self.last_error = f"A topic is missing on {cluster}: {msg.error().str()}"
                else:
                    self.last_error = f"{cluster}: {msg.error().str()}"
                continue
            self._handle(msg, cluster)
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass

    def _handle(self, msg, cluster):
        now = time.time()
        try:
            payload = json.loads(msg.value())
            seq = int(payload["seq"])
        except Exception:
            payload, seq = {}, None
        with self.lock:
            status = self.checker.check(seq, now) if seq is not None else "other"
            latency = None
            if payload.get("created_ms"):
                latency = int(now * 1000) - int(payload["created_ms"])
                self.checker.latencies.append(latency)
        rec = {
            "seq": seq,
            "read_from": cluster,
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "written_to": payload.get("written_to", "?"),
            "created": time.strftime("%H:%M:%S", time.localtime(payload["created_ms"] / 1000)) if payload.get("created_ms") else "",
            "received": time.strftime("%H:%M:%S", time.localtime(now)),
            "latency_ms": latency,
            "status": status,
        }
        data = json.dumps(rec)
        for q in list(self.clients):
            try:
                q.put_nowait(data)
            except queue.Full:
                pass

    def status(self):
        now = time.time()
        with self.lock:
            ch = self.checker
            missing, ranges = ch.missing(now)
            lat = list(ch.latencies)
            return {
                "running": self.running,
                "topic": self.topic,
                "group": self.group,
                "reading": self.reading,
                "received": ch.received,
                "unique": len(ch.seen),
                "duplicates": ch.duplicates,
                "missing": missing,
                "missing_ranges": ranges[-12:],
                "missing_range_count": len(ranges),
                "late": ch.late,
                "highest": ch.max_seq or None,
                "latency_avg": round(sum(lat) / len(lat)) if lat else None,
                "latency_max": max(lat) if lat else None,
                "last_error": self.last_error,
                "grace": GRACE_SEC,
            }

    def reset(self):
        with self.lock:
            self.checker.reset()
            self.last_error = ""


reader = Reader()


# ---- web ----------------------------------------------------------------
def valid_topic(name):
    return bool(name) and len(name) <= 200 and all(ch.isalnum() or ch in "._-" for ch in name)


@app.get("/")
def index():
    return render_template("consumer.html")


@app.get("/api/status")
def api_status():
    return jsonify({"cluster": cm.status(), "reader": reader.status()})


@app.get("/api/topics")
def api_topics():
    try:
        topics = c.list_topics(cm.active)
        prefix = c.PRIMARY_ALIAS + "."
        if c.SAME_NAMES:
            names = topics
        else:
            names = sorted({t[len(prefix):] if t.startswith(prefix) else t for t in topics})
        return jsonify({"topics": names})
    except Exception as exc:
        return jsonify({"topics": [], "error": str(exc)})


@app.post("/api/start")
def api_start():
    body = request.get_json(force=True)
    topic = (body.get("topic") or "").strip()
    group = (body.get("group") or "lab-ui-consumer").strip()
    if not valid_topic(topic) or not valid_topic(group):
        return jsonify({"ok": False, "message": "Enter a topic and a group name (letters, numbers, . _ -)."}), 400
    return jsonify({"ok": True, "message": reader.start(topic, group)})


@app.post("/api/stop")
def api_stop():
    return jsonify({"ok": True, "message": reader.stop()})


@app.post("/api/reset")
def api_reset():
    reader.reset()
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


@app.get("/api/stream")
def api_stream():
    """Server-sent events: batches of received messages for the live table."""
    q = queue.Queue(maxsize=5000)
    reader.clients.append(q)

    def gen():
        try:
            yield "retry: 2000\n\n"
            while True:
                try:
                    batch = [q.get(timeout=15)]
                except queue.Empty:
                    yield ": ping\n\n"
                    continue
                time.sleep(0.2)
                while len(batch) < 300:
                    try:
                        batch.append(q.get_nowait())
                    except queue.Empty:
                        break
                yield "data: [" + ",".join(batch) + "]\n\n"
        finally:
            if q in reader.clients:
                reader.clients.remove(q)

    return Response(gen(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    cm.start()
    app.run(host=c.UI_HOST, port=PORT, threaded=True)
