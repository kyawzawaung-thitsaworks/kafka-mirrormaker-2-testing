# Kafka DR lab: producer and consumer apps

Two small web apps and a lag-test tool for testing MirrorMaker 2 failover.
Everything runs on **kafka-backup**, next to MirrorMaker.

| File | What it is |
|---|---|
| `producer_app.py` | Producer UI (port 8080): create topics, start and stop numbered messages |
| `consumer_app.py` | Consumer UI (port 8081): live messages, with duplicates, missing and late messages counted |
| `common.py` | Shared code: health checks and automatic failover |
| `webauth.py` | Optional login for the UIs |
| `lag-test.sh` | Measures MirrorMaker lag (`measure`) or shows it live (`watch`) |
| `slow-link.sh` | Simulates a slow network between the sites, so lag builds up |
| `lab.env` | Settings: Kafka addresses, topic naming, failover timing, ports |
| `setup.sh` | Installs Kafka on kafka (`primary`); Kafka, MirrorMaker and the apps on kafka-backup (`backup`) |
| `run.sh` | Starts, stops and checks the apps |

## How it works

- **Same topic names on both clusters.** MirrorMaker copies `orders` on the primary into `orders`
  on the backup (no `primary.` prefix). Apps use the same topic name before and after failover.
- Both apps check both clusters every 2 seconds. When the **primary fails 3 checks in a row**,
  each app switches to the backup **by itself** (about 10–15 seconds after the primary dies).
- **Producer:** writes to `orders` on the active cluster. Messages the primary never confirmed are
  sent again to the backup, so nothing the app knows about is lost (but duplicates are possible).
- **Consumer:** reads `orders` on the active cluster with `auto.offset.reset=earliest`. On the
  backup it continues from the bookmark MirrorMaker copied.
- Every message has a number, so the consumer can count **duplicates**, **missing** numbers
  (a gap older than 5 seconds) and **late** arrivals (a gap filled later).
- **Switching back is manual** ("Switch back to primary" button), on purpose: an automatic switch
  back could flip-flop while the primary is unstable. Each app switches on its own, so press the
  button in both UIs.

The apps also support MirrorMaker's default naming (`orders` → `primary.orders`). Set
`REPLICATION_POLICY="default"` in `lab.env` if you keep that. The setting must always match
MirrorMaker's.

## 0. Install Kafka and MirrorMaker 

Skip this if Kafka and MirrorMaker are already running on your VMs. `setup.sh` installs them on
new VMs: Java, Kafka 4.1.0 in `~/kafka`, a single-node `config/lab.properties` and, on the backup,
`mm2.properties` already set up for the lab (one direction only, same topic names, consumer
bookmarks copied).

**a) Copy the bundle to both VMs (in Cloud Shell).** Upload `kafka-dr-lab.tar.gz` with
**⋮ (More) → Upload**, then:

```bash
gcloud compute scp kafka-dr-lab.tar.gz kafka:~
gcloud compute scp kafka-dr-lab.tar.gz kafka-backup:~
```

Port 9092 must be allowed from kafka-backup to kafka, so MirrorMaker and the apps can reach the
primary.

**b) On kafka** (first):

```bash
tar -xzf kafka-dr-lab.tar.gz
cd kafka-dr-lab
./setup.sh primary
```

It ends with `Setup done.` and Kafka running on port 9092.

**c) On kafka-backup:**

```bash
tar -xzf kafka-dr-lab.tar.gz
cd kafka-dr-lab
./setup.sh backup
```

This installs Kafka, writes `mm2.properties`, starts MirrorMaker and sets up the apps.

**d) Check it (on kafka-backup)**, about 30 seconds later:

```bash
cd ~/kafka
bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic namecheck --partitions 3 --replication-factor 1
sleep 20
bin/kafka-topics.sh --bootstrap-server kafka-backup:9092 --list
```

You should see `namecheck` and `primary.heartbeats` on the backup. If not, look at
`tail -30 ~/kafka/mm2.log`.

Running `setup.sh` again is safe: anything already installed, configured or running is kept.
To use another Kafka version: `KAFKA_VERSION=4.1.0 ./setup.sh primary`. If your VMs have other
names, add `PRIMARY_VM=... BACKUP_VM=...` in front, and change the addresses in `lab.env` to match.

After this, steps 1 to 3 are already done. Continue with step 4.

## 1. Set MirrorMaker to keep the same topic names (once)

If you installed with `./setup.sh backup` (step 0), this is already done. Skip to step 4.

**Start fresh.** MirrorMaker remembers how far it has copied each topic. If you only change the
setting, the new same-name copies start from where the old copies stopped, and older messages
are missing from them. In the lab, the simplest fix is a clean start of both clusters.

**On kafka-backup:** stop MirrorMaker and wait until it's gone:

```bash
pkill -f MirrorMaker
while pgrep -f MirrorMaker > /dev/null; do sleep 1; done; echo "copier stopped"
```

**On both VMs:** clean start of Kafka:

```bash
cd ~/kafka
bin/kafka-server-stop.sh
sleep 10
rm -rf ~/kafka-data
bin/kafka-storage.sh format --standalone -t $(bin/kafka-storage.sh random-uuid) -c config/lab.properties
bin/kafka-server-start.sh -daemon config/lab.properties
```

**On kafka-backup:** add the naming rule to `mm2.properties` and start MirrorMaker:

```bash
cd ~/kafka
echo "replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy" >> mm2.properties
grep -E "policy|->" mm2.properties
nohup bin/connect-mirror-maker.sh mm2.properties > mm2.log 2>&1 &
```

The `grep` must show the new line **and** `backup->primary.enabled = false`. With same names,
MirrorMaker can't tell a copy from an original, so copying in both directions at once would send
messages back and forth forever. Only ever copy in **one direction at a time**.

**Check it**, about 30 seconds later:

```bash
bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic namecheck --partitions 3 --replication-factor 1
sleep 20
bin/kafka-topics.sh --bootstrap-server kafka-backup:9092 --list
```

You should see `namecheck` on the backup, **not** `primary.namecheck`. You'll still see
`primary.heartbeats`: heartbeats keep the prefix so you can tell which cluster they came from.

## 2. Copy the bundle to kafka-backup

In **Cloud Shell**: click **⋮ (More) → Upload** and upload `kafka-dr-lab.tar.gz`. Then:

```bash
gcloud compute scp kafka-dr-lab.tar.gz kafka-backup:~
gcloud compute ssh kafka-backup
```

## 3. Set up the apps (once)

**On kafka-backup:**

```bash
tar -xzf kafka-dr-lab.tar.gz
cd kafka-dr-lab
./setup.sh
```

Check `lab.env`. The defaults match the lab: `kafka:9092`, `kafka-backup:9092`, alias `primary`,
and `REPLICATION_POLICY="identity"` (same names). MirrorMaker must have
`sync.group.offsets.enabled = true` (it does in the lab config).

The UIs listen on the VM's private IP with no password (`UI_HOST="0.0.0.0"`, `UI_PASSWORD=""`).
That's fine while the VM has only a private IP, because only machines on the network or VPN can
reach it.

## 4. Start the apps

```bash
./run.sh start      # also: stop, restart, status, logs
```

`run.sh` prints the address of each UI, for example:

```
producer app: running (pid 1234)
    http://10.110.10.8:8080   (private IP: open it over the VPN)
consumer app: running (pid 1235)
    http://10.110.10.8:8081   (private IP: open it over the VPN)
```

## 5. Open the UIs over the VPN

**a) Allow the UI ports (once, in Cloud Shell):**

```bash
gcloud compute instances add-tags kafka-backup --tags=lab-ui
gcloud compute firewall-rules create allow-lab-ui \
  --network=default --direction=INGRESS --allow=tcp:8080-8081 \
  --source-ranges=0.0.0.0/0 --target-tags=lab-ui
```

If your VMs aren't on the `default` network, change `--network`. Because kafka-backup has no
public IP, `0.0.0.0/0` still only lets in machines that can reach the private network.

**b) Connect your VPN,** then open in your browser:

- Producer: `http://10.110.10.8:8080`
- Consumer: `http://10.110.10.8:8081`

(Use the private IP that `./run.sh status` prints. Use `http://`, not `https://`.)

**If the VM ever gets a public IP,** the rule above would open the UIs to the internet. Then set
`UI_PASSWORD` in `lab.env` and run `./run.sh restart`, or delete the rule. `run.sh` warns you when
the VM has a public IP and no password is set.

---

## Test A: automatic failover

1. **Producer UI:** create topic `orders`, then **Start generating** (20 per second).
2. **Consumer UI:** topic `orders`, group `lab-ui-consumer`, **Start reading**.
3. Wait about 30 seconds, so MirrorMaker copies the data and the consumer's bookmark.
4. **On kafka (tab 2):** crash the primary: `pkill -9 -f lab.properties`
5. Watch both UIs. Within about 15 seconds:
   - the route board moves to **Backup**, and "What happened" shows the failed checks and the switch
   - the producer shows **Confirmed by backup** going up, and **Sent again** for the unconfirmed messages
   - the consumer reads `orders on the backup`, with some **duplicates** and normally **0 missing**
     (it had already read those messages from the primary)
6. **Bring the primary back (tab 2):** `cd ~/kafka && bin/kafka-server-start.sh -daemon config/lab.properties`
   - MirrorMaker is still running, so it now copies the messages it missed before the crash into
     the **same** `orders` topic the producer is writing to. The consumer may show them as **late**.
     In production you would stop MirrorMaker at failover to avoid this mixing.
7. **Switch back:** click **Switch back to primary** in **both** UIs. Messages written to the
   backup during the outage stay only in the backup.

## Test B: missing data (lag + crash)

The consumer only sees missing messages if it was **behind** when the primary died, for example
a reader that starts after the failover.

1. Turn on the slow link (on kafka-backup): `./slow-link.sh on 0.02` (0.02 MB per second)
2. Producer: topic `payments`, **Start generating** at 500 per second (about 0.06 MB per second,
   faster than the link). The **Mirror lag** panel grows.
3. Crash the primary (tab 2): `pkill -9 -f lab.properties`. Wait until both UIs show **Backup**.
4. Consumer: stop reading, **Clear counts**, then read `payments` with a **new group** (for example `audit`).
5. **Missing** shows the messages that were saved in the primary but never copied, with their numbers.
6. Clean up: start the primary again, then `./slow-link.sh off`

## Test C: measure the lag

```bash
./lag-test.sh measure                              # 2,000 messages at 200 per second
./lag-test.sh measure --count 20000 --rate 2000    # a busier test
./slow-link.sh on 0.2
./lag-test.sh measure --count 20000 --rate 2000    # the same over a slow link
./slow-link.sh off
```

Lag only builds up when messages arrive faster than the link: 2,000 messages per second of about
200 bytes is roughly 0.4 MB per second, so a 0.2 MB per second link falls behind.

`measure` sends numbered messages to the primary and times how long each one takes to appear in
the backup. It prints min, average, p95, p99 and max delay, and how many messages never arrived.

To watch any topic live (Ctrl+C to stop):

```bash
./lag-test.sh watch --topic orders
```

Run tests B and C **before** a failover, or after switching back. Once apps write to the backup,
the backup topic has more messages than the primary, so lag can't be worked out.

---

## Settings (`lab.env`)

| Setting | Default | Meaning |
|---|---|---|
| `PRIMARY_BOOTSTRAP`, `BACKUP_BOOTSTRAP` | `kafka:9092`, `kafka-backup:9092` | Kafka addresses |
| `PRIMARY_ALIAS` | `primary` | First name in `clusters = primary, backup` in `mm2.properties` |
| `REPLICATION_POLICY` | `identity` | `identity` = same topic names; `default` = copies named `primary.<topic>`. Must match MirrorMaker |
| `CHECK_INTERVAL_SEC` | `2` | Seconds between health checks |
| `CHECK_TIMEOUT_SEC` | `3` | How long one check waits for an answer |
| `FAIL_THRESHOLD` | `3` | Failed checks in a row before failing over |
| `MISSING_GRACE_SEC` | `5` | A gap counts as missing after this many seconds |
| `UI_HOST` | `0.0.0.0` | `0.0.0.0` = reachable through the VM's IP (over the VPN); `127.0.0.1` = only from the VM itself |
| `PRODUCER_PORT`, `CONSUMER_PORT` | `8080`, `8081` | Ports of the UIs |
| `UI_USER`, `UI_PASSWORD` | `lab`, empty | Optional login for the UIs. Empty password = no login |
| `KAFKA_HOME` | `$HOME/kafka` | Used by `slow-link.sh` |

After changing settings: `./run.sh restart`

## If something doesn't work

| Problem | What to check |
|---|---|
| A page doesn't open (waits, then times out) | The VPN is connected; `./run.sh status`; the firewall rule exists (`gcloud compute firewall-rules describe allow-lab-ui`); kafka-backup has the `lab-ui` tag; you used `http://`, not `https://` |
| The browser asks for a password | `UI_PASSWORD` is set in `lab.env`. User name is `UI_USER` (default `lab`). Empty it and run `./run.sh restart` for no login |
| Both clusters show "Not responding" | Kafka addresses in `lab.env`; run `./run.sh logs` |
| Producer says "primary.orders exists on the backup" | MirrorMaker still adds the prefix: check step 1, or set `REPLICATION_POLICY="default"` |
| "In the backup" stays at 0 | MirrorMaker is running (`pgrep -f MirrorMaker`) and uses the same naming as `lab.env` |
| Consumer shows "A topic is missing on backup" | Create the topic with the producer app, which also creates it on the backup |
| Lag is negative | Something has written to the backup topic (normal after a failover), or the topic was recreated on the primary. Use a new topic name for lag tests |
| `slow-link.sh` can't find Kafka | Set `KAFKA_HOME` in `lab.env` |

## Notes

- **Automatic failover is for this lab.** In production, a broken network between the sites can
  look like a dead primary, and automatic failover could then split writes between both clusters
  (split brain). That's why production failover is usually a human decision.
- **Same names means one direction at a time.** For failback, stop the primary → backup copier
  first, then copy backup → primary. The backup topic holds the old copied data plus the new
  messages, so copying it all back would give the primary the old data twice. Teams usually empty
  or recreate the primary's topics first.
- The slow link limits **every** consumer of the primary, including the consumer app while it
  reads from the primary.
