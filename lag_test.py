#!/usr/bin/env python3
"""Measure how far MirrorMaker 2 is behind when copying primary -> backup.

  measure  Sends numbered test messages to the primary and times how long
           each one takes to appear in the backup copy of the topic.
  watch    Shows, every few seconds, how many messages a topic has in the
           primary, how many are copied to the backup, and the difference.

Examples:
  ./lag_test.py measure                          # 2,000 messages at 200/s
  ./lag_test.py measure --count 20000 --rate 2000
  ./lag_test.py watch --topic orders

Run it on kafka-backup, so sending and receiving use the same clock.
"""
import argparse
import json
import statistics
import sys
import threading
import time
import uuid

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

import common as c


def wait_for_topic(admin, topic, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            md = admin.list_topics(topic, timeout=3)
            t = md.topics.get(topic)
            if t is not None and t.error is None and t.partitions:
                return sorted(t.partitions)
        except Exception:
            pass
        time.sleep(2)
    return None


def pct(values, p):
    if not values:
        return 0
    values = sorted(values)
    k = min(len(values) - 1, max(0, round(p / 100 * (len(values) - 1))))
    return values[k]


def measure(args):
    topic = args.topic
    remote = c.remote_topic(topic)
    run_id = uuid.uuid4().hex[:8]
    print(f"Primary: {c.PRIMARY_BOOTSTRAP}   Backup: {c.BACKUP_BOOTSTRAP}")
    print(f"Test topic: {topic} (copy on the backup: {remote}), run {run_id}\n")

    print(c.create_topic("primary", topic, args.partitions))
    backup_admin = AdminClient(c.client_conf("backup"))
    print(f"Waiting for MirrorMaker to create {remote} on the backup…")
    parts = wait_for_topic(backup_admin, remote, args.timeout)
    if parts is None:
        sys.exit(f"{remote} did not appear on the backup within {args.timeout}s. Is MirrorMaker running?")

    # Start reading the backup copy from its current end, before sending.
    consumer = Consumer(c.client_conf("backup", **{"group.id": f"lag-test-{run_id}", "enable.auto.commit": False}))
    start = []
    for p in parts:
        _low, high = consumer.get_watermark_offsets(TopicPartition(remote, p), timeout=10, cached=False)
        start.append(TopicPartition(remote, p, high))
    consumer.assign(start)

    sent_ns, acked_ns, recv_ns = {}, {}, {}
    stop = threading.Event()

    def read():
        while not stop.is_set():
            msg = consumer.poll(0.2)
            if msg is None or msg.error():
                continue
            try:
                body = json.loads(msg.value())
            except Exception:
                continue
            if body.get("run") == run_id and body["seq"] not in recv_ns:
                recv_ns[body["seq"]] = time.time_ns()

    reader = threading.Thread(target=read, daemon=True)
    reader.start()

    producer = Producer(c.client_conf("primary", **{"acks": "all", "linger.ms": 5}))

    def on_ack(err, msg, seq):
        if err is None:
            acked_ns[seq] = time.time_ns()

    pad = "x" * max(0, args.size - 80)
    interval = 1.0 / args.rate if args.rate > 0 else 0
    print(f"Sending {args.count:,} messages of about {args.size} bytes"
          + (f" at {args.rate:g} per second…" if args.rate > 0 else " as fast as possible…"))
    t0 = time.monotonic()
    next_t, last_print = t0, t0

    def progress(prefix=""):
        a, r = len(acked_ns), len(recv_ns)
        print(f"{prefix}{time.strftime('%H:%M:%S')}  sent {len(sent_ns):>8,}  in primary {a:>8,}  "
              f"copied to backup {r:>8,}  not copied yet {a - r:>8,}", flush=True)

    for seq in range(1, args.count + 1):
        sent_ns[seq] = time.time_ns()
        while True:
            try:
                producer.produce(topic, key=str(seq),
                                 value=json.dumps({"run": run_id, "seq": seq, "pad": pad}),
                                 on_delivery=lambda err, msg, s=seq: on_ack(err, msg, s))
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        if interval:
            next_t += interval
            delay = next_t - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        if time.monotonic() - last_print >= 1:
            progress()
            last_print = time.monotonic()
    producer.flush(30)
    progress()

    print("\nAll sent. Waiting for the backup to catch up…")
    last_count, last_change = -1, time.monotonic()
    while len(recv_ns) < len(acked_ns):
        if len(recv_ns) != last_count:
            last_count, last_change = len(recv_ns), time.monotonic()
        elif time.monotonic() - last_change > args.timeout:
            print(f"No progress for {args.timeout}s, stopping.")
            break
        time.sleep(1)
        progress()
    stop.set()
    reader.join(timeout=2)
    consumer.close()

    # ---- results ----
    both = [s for s in acked_ns if s in recv_ns]
    # Confirmations are handled in batches, so a confirmation can be recorded a
    # few ms after the copy already arrived. Count those as 0 ms.
    repl = [max(0.0, (recv_ns[s] - acked_ns[s]) / 1e6) for s in both]
    e2e = [(recv_ns[s] - sent_ns[s]) / 1e6 for s in both]
    missing = len(acked_ns) - len(both)
    catch_up = (max(recv_ns.values()) - max(acked_ns.values())) / 1e9 if both else 0

    print("\n===== Result =====")
    print(f"Sent:                  {args.count:>9,}")
    print(f"Confirmed by primary:  {len(acked_ns):>9,}")
    print(f"Arrived in backup:     {len(both):>9,}")
    print(f"Missing in backup:     {missing:>9,}")
    if repl:
        print("\nDelay from 'saved in primary' to 'readable in backup' (milliseconds):")
        print(f"  min {min(repl):8.0f}   avg {statistics.mean(repl):8.0f}   p50 {pct(repl, 50):8.0f}")
        print(f"  p95 {pct(repl, 95):8.0f}   p99 {pct(repl, 99):8.0f}   max {max(repl):8.0f}")
        print(f"\nEnd-to-end delay (send -> readable in backup): avg {statistics.mean(e2e):.0f} ms, max {max(e2e):.0f} ms")
        print(f"Backup caught up {max(catch_up, 0):.1f} s after the last message was saved in the primary.")
        print("\nWhat it means: if the primary had died at the worst moment, about the last "
              f"{max(repl) / 1000:.1f} s of messages would not have been in the backup.")


def watch(args):
    counters = {name: c.Counter(name) for name in c.CLUSTERS}
    topic, remote = args.topic, c.remote_topic(args.topic)
    print(f"Watching {topic} on the primary and {remote} on the backup. Press Ctrl+C to stop.\n")
    biggest = 1
    try:
        while True:
            a = counters["primary"].total(topic)
            b = counters["backup"].total(remote)
            if a is None or b is None:
                which = "primary" if a is None else "backup"
                print(f"{time.strftime('%H:%M:%S')}  {which} not reachable, or the topic is missing there"
                      f"   (primary {a if a is not None else '–'}, backup {b if b is not None else '–'})")
            else:
                lag = a - b
                biggest = max(biggest, lag)
                bar = "#" * int(40 * lag / biggest) if lag > 0 else ""
                if lag < 0:
                    bar = "(backup has more: something writes to the backup topic, e.g. after a failover)"
                print(f"{time.strftime('%H:%M:%S')}  primary {a:>9,}   backup {b:>9,}   lag {lag:>8,}  {bar}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    m = sub.add_parser("measure", help="time how long messages take to reach the backup")
    m.add_argument("--topic", default="lagtest")
    m.add_argument("--count", type=int, default=2000)
    m.add_argument("--rate", type=float, default=200, help="messages per second, 0 = as fast as possible")
    m.add_argument("--size", type=int, default=200, help="approximate message size in bytes")
    m.add_argument("--partitions", type=int, default=3)
    m.add_argument("--timeout", type=int, default=60, help="seconds to wait without progress")
    m.set_defaults(fn=measure)

    w = sub.add_parser("watch", help="show primary vs backup message counts for a topic")
    w.add_argument("--topic", required=True)
    w.add_argument("--interval", type=float, default=2)
    w.set_defaults(fn=watch)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
