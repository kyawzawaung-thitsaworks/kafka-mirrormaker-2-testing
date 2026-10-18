"""Shared code for the producer and consumer apps.

- Cluster settings (read from environment variables, see lab.env)
- Health checks of both Kafka clusters
- Automatic failover from the primary to the backup
- Small helpers for topics and message counts
"""
import os
import threading
import time
from collections import deque
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

PRIMARY_BOOTSTRAP = os.environ.get("PRIMARY_BOOTSTRAP", "kafka:9092")
BACKUP_BOOTSTRAP = os.environ.get("BACKUP_BOOTSTRAP", "kafka-backup:9092")
# Must match the first name in "clusters = primary, backup" in mm2.properties.
PRIMARY_ALIAS = os.environ.get("PRIMARY_ALIAS", "primary")
# Must match replication.policy.class in mm2.properties:
#   identity = same topic names on both clusters (IdentityReplicationPolicy): orders -> orders
#   default  = MirrorMaker adds the alias (DefaultReplicationPolicy):        orders -> primary.orders
REPLICATION_POLICY = os.environ.get("REPLICATION_POLICY", "identity").strip().lower()
SAME_NAMES = REPLICATION_POLICY == "identity"

CHECK_INTERVAL = float(os.environ.get("CHECK_INTERVAL_SEC", "2"))
CHECK_TIMEOUT = float(os.environ.get("CHECK_TIMEOUT_SEC", "3"))
FAIL_THRESHOLD = int(os.environ.get("FAIL_THRESHOLD", "3"))
REPLICATION_FACTOR = int(os.environ.get("TOPIC_REPLICATION_FACTOR", "1"))
UI_HOST = os.environ.get("UI_HOST", "127.0.0.1")

CLUSTERS = {"primary": PRIMARY_BOOTSTRAP, "backup": BACKUP_BOOTSTRAP}


def _ignore_error(err):
    """Connection errors are expected while a cluster is down; the health
    checker reports them, so we keep the client libraries quiet."""


def client_conf(cluster, **extra):
    conf = {
        "bootstrap.servers": CLUSTERS[cluster],
        "log_level": 0,
        "enable.metrics.push": False,
        "error_cb": _ignore_error,
    }
    conf.update(extra)
    return conf


def now_ms():
    return int(time.time() * 1000)


def clock():
    return datetime.now().strftime("%H:%M:%S")


def remote_topic(topic):
    """Name of the copy that MirrorMaker 2 makes on the backup."""
    return topic if SAME_NAMES else f"{PRIMARY_ALIAS}.{topic}"


def prefixed_copy(topic):
    """Name the copy would have with MirrorMaker's default naming."""
    return f"{PRIMARY_ALIAS}.{topic}"


def read_topics(topic, cluster):
    """Topics a consumer must read on each cluster.

    On the primary: just the topic.
    On the backup, with same names: just the topic too (the copy and the
    messages written after failover are in the same topic).
    On the backup, with default naming: the copy made by MirrorMaker
    (old data) AND the topic producers write to after failover (new data).
    """
    if cluster == "primary" or SAME_NAMES:
        return [topic]
    return [remote_topic(topic), topic]


def is_internal(name):
    return (
        name.startswith("__")
        or name.startswith("mm2-")
        or name.endswith(".internal")
        or name in ("heartbeats", f"{PRIMARY_ALIAS}.heartbeats")
    )


class ClusterManager:
    """Checks both clusters every few seconds and decides which one the app
    uses. Switches to the backup automatically after FAIL_THRESHOLD failed
    checks of the primary in a row. Switching back (failback) is manual on
    purpose: an automatic switch back could flip-flop while the primary is
    unstable."""

    def __init__(self, app_name):
        self.app_name = app_name
        self.active = "primary"
        self.health = {"primary": None, "backup": None}
        self.failures = 0
        self.auto_failover = True
        self.switched_at = None
        self.events = deque(maxlen=60)
        self._listeners = []
        self._lock = threading.RLock()
        self._admins = {name: AdminClient(client_conf(name)) for name in CLUSTERS}
        naming = "same topic names on both clusters" if SAME_NAMES else f"copies named {PRIMARY_ALIAS}.<topic>"
        self.log(f"Started. Primary {PRIMARY_BOOTSTRAP}, backup {BACKUP_BOOTSTRAP}, {naming}")

    # ---- public -------------------------------------------------------
    def start(self):
        threading.Thread(target=self._loop, name="health-check", daemon=True).start()

    def on_switch(self, fn):
        """fn(old_cluster, new_cluster) is called after every switch."""
        self._listeners.append(fn)

    def admin(self, cluster=None):
        return self._admins[cluster or self.active]

    def log(self, msg, level="info"):
        self.events.appendleft({"time": clock(), "msg": msg, "level": level})

    def set_auto_failover(self, enabled):
        with self._lock:
            self.auto_failover = bool(enabled)
        self.log(f"Automatic failover turned {'on' if enabled else 'off'}")

    def switch_to(self, cluster, reason):
        with self._lock:
            if cluster == self.active:
                return False
            old = self.active
            self.active = cluster
            self.failures = 0
            self.switched_at = clock()
        level = "alert" if cluster == "backup" else "info"
        self.log(f"Switched from {old} to {cluster}: {reason}", level)
        for fn in self._listeners:
            try:
                fn(old, cluster)
            except Exception as exc:  # keep the health checker alive
                self.log(f"Switch handler error: {exc}", "warn")
        return True

    def status(self):
        with self._lock:
            return {
                "app": self.app_name,
                "active": self.active,
                "bootstrap": CLUSTERS,
                "health": dict(self.health),
                "failures": self.failures,
                "fail_threshold": FAIL_THRESHOLD,
                "auto_failover": self.auto_failover,
                "switched_at": self.switched_at,
                "primary_alias": PRIMARY_ALIAS,
                "same_names": SAME_NAMES,
                "events": list(self.events),
            }

    # ---- health checks ------------------------------------------------
    def probe(self, cluster):
        admin = self._admins[cluster]
        try:
            md = admin.list_topics(timeout=CHECK_TIMEOUT)
            return len(md.brokers) > 0
        except Exception:
            return False
        finally:
            admin.poll(0)

    def _loop(self):
        while True:
            p_ok = self.probe("primary")
            b_ok = self.probe("backup")
            switch = False
            with self._lock:
                old = dict(self.health)
                self.health = {"primary": p_ok, "backup": b_ok}
                if old["primary"] is not None and old["primary"] != p_ok:
                    self.log(
                        "Primary is reachable again" if p_ok else "Primary is not responding",
                        "info" if p_ok else "warn",
                    )
                if old["backup"] is not None and old["backup"] != b_ok:
                    self.log(
                        "Backup is reachable again" if b_ok else "Backup is not responding",
                        "info" if b_ok else "warn",
                    )
                if self.active == "primary":
                    if p_ok:
                        self.failures = 0
                    else:
                        self.failures += 1
                        self.log(f"Primary failed check {self.failures} of {FAIL_THRESHOLD}", "warn")
                        if self.failures >= FAIL_THRESHOLD:
                            if not self.auto_failover:
                                if self.failures == FAIL_THRESHOLD:
                                    self.log("Automatic failover is off. Fail over by hand if needed.", "warn")
                            elif b_ok:
                                switch = True
                            else:
                                self.log("Backup is not reachable either. Staying on primary.", "warn")
            if switch:
                self.switch_to("backup", f"primary failed {FAIL_THRESHOLD} checks in a row")
            time.sleep(CHECK_INTERVAL)


# ---- topics and counts --------------------------------------------------
def create_topic(cluster, name, partitions):
    """Create a topic. Returns a short result message."""
    admin = AdminClient(client_conf(cluster))
    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=REPLICATION_FACTOR)])
    try:
        futures[name].result(timeout=10)
        return f"created {name} on {cluster}"
    except KafkaException as exc:
        err = exc.args[0]
        if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
            return f"{name} already exists on {cluster}"
        return f"could not create {name} on {cluster}: {err.str()}"
    except Exception as exc:
        return f"could not create {name} on {cluster}: {exc}"


def list_topics(cluster, timeout=3):
    admin = AdminClient(client_conf(cluster))
    md = admin.list_topics(timeout=timeout)
    return sorted(t for t in md.topics if not is_internal(t))


class Counter:
    """Counts messages in a topic (sum of end offsets of its partitions)."""

    def __init__(self, cluster):
        self.cluster = cluster
        self._c = Consumer(client_conf(cluster, **{"group.id": "lab-message-counter", "enable.auto.commit": False}))

    def total(self, topic, timeout=2.0):
        """Returns the number of messages, or None if the topic does not
        exist or the cluster cannot be reached."""
        try:
            md = self._c.list_topics(topic, timeout=timeout)
            t = md.topics.get(topic)
            if t is None or t.error is not None:
                return None
            total = 0
            for p in t.partitions:
                _low, high = self._c.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout, cached=False)
                total += high
            return total
        except Exception:
            return None
