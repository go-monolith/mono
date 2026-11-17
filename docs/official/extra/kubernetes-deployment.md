# ☸️ Kubernetes Deployment Strategies

How a mono application whose embedded NATS servers form one JetStream cluster behaves during rolling updates, canaries, bad releases, node drains and blue-green cutovers on Kubernetes, with measured results and the exact commands that worked.

The baseline setup (StatefulSet, headless Service, routes, probes, PodDisruptionBudget) is described in the [Clustering and Kubernetes guide](../guides/clustering.md#kubernetes) and implemented in the [Kubernetes cluster example](../../../examples/k8s-cluster/README.md). This page is about **changing** a running deployment.

{% hint style="info" %}
**Short version.** Run 3 servers with `WithJetStreamDefaultReplicas(3)`, use plain rolling updates (with `partition` for canaries), and keep a `preStop` sleep and a PodDisruptionBudget of `minAvailable: 2`. Every strategy below that keeps two of the three servers up loses no requests. Blue-green needs a migration step and is only worth it when you need a fast switch back.
{% endhint %}

## Results at a Glance

Measured with the Kubernetes cluster example: 3 pods on 3 nodes, every stream and consumer with 3 replicas, 5 requests per second through the Service. Each request stores a uniquely named visit in a replicated stream, and every visit is tracked until a pod handles it. After all runs the visits consumer had no pending and no unacknowledged messages, and its ack floor was the stream's last sequence.

| Strategy | Requests failed | Visits lost / duplicated / replayed | Cron |
|---|---|---|---|
| [Rolling update](#rolling-update-recommended) to a new image | 0 of 268 | 0 / 0 / 0 | every 15 s |
| [Rolling restart](#rolling-update-recommended) (`kubectl rollout restart`) | 0 of 268 | 0 / 0 / 0 | every 15 s |
| [Canary](#canary-with-partition) (`partition`), 60 s soak, then promote | 0 of 755 | 0 / 0 / 0 | every 15 s |
| [Bad release](#bad-release-and-rollback) (crash loop), then `kubectl rollout undo` | 0 of 624 | 0 / 0 / 0 | every 15 s |
| [Node drain](#node-maintenance), then a second drain | 0 of 551 | 0 / 0 / 0 | every 15 s |
| [One pod down](#losing-pods) for about 40 s | 0 of 469 | 0 / 0 / 0 | every 15 s |
| [Two pods down](#losing-pods) for about 30 s | 39 of 549 (26.8 s of `502`) | 0 / 0 / 0 | paused, then resumed |
| Scaling a 2-replica deployment to 3 in place | 1 of 389 | 0 / 0 / 0 | every 15 s |
| [Blue-green, separate clusters](#separate-clusters) | 0 of 1166 | data split between the colours | fires in **both** while both run |
| [Blue-green, joined cluster](#joined-cluster), old colour deleted without migrating | 38 of 671 (30.6 s of `502`) | 0 / 0 / 0 | gap |
| [Blue-green, joined cluster](#joined-cluster), with the cutover runbook | 0 of 541 | 0 / 0 / 0 | every 15 s |
| [Blue-green, joined cluster](#joined-cluster), reverse cutover | 0 of 669 | 0 / 0 / 0 | every 15 s |

With **2 servers** the same rolling update failed requests for 4–8 s per restarted pod, and a crash-looping release left JetStream unavailable for as long as it was out (47.6 s in the test): two servers have no quorum while either is down.

See [Methodology and Limits](#methodology-and-limits) for what these numbers do and do not cover.

## Before You Deploy

The behaviour above depends on these settings, all in the example manifest:

- **3 servers (or another odd number), one per node**, and `WithJetStreamDefaultReplicas(3)`, so every stream and consumer keeps a quorum while one server is down.
- **A PodDisruptionBudget with `minAvailable: 2`**, so voluntary disruptions take down one pod at a time.
- **A `preStop` sleep** (5 s), so a terminating pod keeps serving until it has left the Service endpoints. Without it, the test saw 2–3 connection errors per rolling update.
- **A liveness probe that never depends on JetStream**, so losing quorum does not restart the surviving pods.
- **`podManagementPolicy: Parallel`** and **`publishNotReadyAddresses: true`** on the headless Service, so pods that start together can form the cluster.

To inspect JetStream during a deployment, port-forward to a pod's NATS client port. The example binds the client listener to loopback, and a port-forward reaches the pod's loopback:

```bash
kubectl -n mono-demo port-forward pod/mono-cluster-0 14222:4222 &
nats -s nats://127.0.0.1:14222 stream ls
nats -s nats://127.0.0.1:14222 stream info GREETER_VISITS     # leader and replicas
nats -s nats://127.0.0.1:14222 consumer info GREETER_VISITS greeter-visits
```

## Rolling Update (Recommended)

```bash
kubectl -n mono-demo set image sts/mono-cluster app=registry.example.com/mono-k8s-cluster:0.2.1
kubectl -n mono-demo rollout status sts/mono-cluster
```

The StatefulSet replaces one pod at a time, highest ordinal first, and waits for each to be Ready. While a pod restarts, the other two keep the quorum, so request-reply, stream writes, consumers and cron keep working. Its returning `Start` waits for JetStream and its replicas to catch up before it reports Ready. The 3 pods were replaced in under 30 s.

What to expect:

- **Both versions run side by side for a while** (about 11 s in the test). Every service is shared through NATS: request-reply calls, stream consumer messages and cron occurrences go to whichever instance takes them, old or new. Keep message formats and handler behaviour compatible across one version step.
- **A few messages are delayed by the ack wait.** A message fetched by a pod that is shutting down is left unacknowledged and is delivered again after the consumer's `AckWait` (30 s by default). The test saw 3 visits handled about 31 s after they were published. They are delayed, not lost or duplicated.
- **Leader moves log one `Fetch error`** per fetch loop at Warn (`nats: leadership change` or `nats: server shutdown`), followed by `Fetch error cleared` within seconds. Up to v0.2.1 these two were logged at Error.

## Canary with Partition

A `partition` limits the update to the pods whose ordinal is at least the partition:

```bash
# Only mono-cluster-2 gets the new image
kubectl -n mono-demo patch sts mono-cluster --type merge \
  -p '{"spec":{"updateStrategy":{"rollingUpdate":{"partition":2}}}}'
kubectl -n mono-demo set image sts/mono-cluster app=registry.example.com/mono-k8s-cluster:0.2.1
kubectl -n mono-demo rollout status sts/mono-cluster

# Watch it, then promote (or set the old image back)
kubectl -n mono-demo patch sts mono-cluster --type merge \
  -p '{"spec":{"updateStrategy":{"rollingUpdate":{"partition":0}}}}'
kubectl -n mono-demo rollout status sts/mono-cluster
```

**The canary's share of traffic is set by NATS, not by the Service.** Request-reply services are queue groups across all instances, so one canary pod out of three answers about a third of the calls, whichever pod received the HTTP request. The test measured 33% (88 of 268). The canary also takes its share of stream consumer messages and cron occurrences. To send it less traffic, you need more stable pods, not a different Service.

## Bad Release and Rollback

A release that never becomes Ready stops the rollout at the first replaced pod (`mono-cluster-2`), and the StatefulSet leaves the other pods alone. With 3 servers the two remaining ones keep the quorum, so nothing fails while the bad pod crash-loops. Roll back with:

```bash
kubectl -n mono-demo rollout undo sts/mono-cluster
kubectl -n mono-demo rollout status sts/mono-cluster
```

In the test (Kubernetes 1.37, `podManagementPolicy: Parallel`), `rollout undo` replaced the crash-looping pod by itself. If a StatefulSet stays stuck on a broken pod after the rollback, delete that pod so it is recreated from the restored template.

The tested release failed on its configuration before its NATS server started, so it never joined the cluster. A release that starts, joins the cluster and then misbehaves (wrong message format, failing handlers) affects every instance's work through the shared services, like a canary. Catch it with a canary first.

On 2 servers, the same bad release made JetStream unavailable for its whole duration.

## Node Maintenance

```bash
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data
# ... maintenance ...
kubectl uncordon <node>
```

The PodDisruptionBudget (`minAvailable: 2`) lets a drain evict one application pod. The evicted pod usually cannot run elsewhere: anti-affinity allows one pod per node, and on storage bound to a node its volume can't move. It stays Pending until the node is uncordoned. A second drain on another node meanwhile cannot evict its pod: `kubectl drain` keeps retrying the eviction until the budget allows it, which is once the first pod is back, so the cluster keeps its quorum. Pass `--timeout` to make it give up instead (the test used `--timeout=30s`). Drain one node at a time, and uncordon it before the next.

## Losing Pods

- **One pod down** (crash, node failure): nothing fails. Leaders on the lost server move within seconds; `/readyz` of the others reports `"available": true`.
- **Two pods down**: no quorum. Request-reply keeps working on the remaining pod, but anything that writes to JetStream fails (`/hello` answered `502` for 26.8 s in the test). `Health().JetStream` reports `"available": false` with "no meta leader (no quorum)", and cron occurrences wait. Once a second pod is back, everything resumes, and messages not yet handled are still in their streams.

## Blue-Green

A blue-green deployment runs the new version (green) next to the old one (blue) and switches the Service selector. With embedded NATS servers the data lives in the pods' JetStream stores, so first decide whether green is a **separate cluster** or **joins** blue's.

| | Separate clusters | Joined cluster |
|---|---|---|
| Downtime | none | none with the runbook; an outage if the old colour is simply deleted |
| Streams and consumers | **not carried over**: green starts empty, and what green stores stays in green | carried over by moving the replicas |
| Request-reply, consumers, cron | each colour serves its own | shared by both colours from the moment green joins |
| Cron during the overlap | fires in **both** clusters | once per occurrence |
| Switching back | flip the selector back, while blue still runs | the reverse runbook, 5 minutes after the old names were removed |
| Extra requirements | none | a system-account user for meta peer removal |

### Separate clusters

Green gets its own StatefulSet, headless Service, volumes and NATS cluster name, and routes only to its own pods. Flip the HTTP Service when green is Ready:

```bash
kubectl -n mono-demo patch svc mono-cluster -p '{"spec":{"selector":{"app":"mono-green"}}}'
```

The test flipped to green, scaled blue to zero, brought blue back and flipped back, with no failed requests. The data, however, did not follow the traffic:

- green's stream started at sequence 1, without blue's history, and blue's unhandled messages stay in blue;
- the 519 visits written while green served stayed in green after the switch back;
- cron fired in both clusters while both ran (occurrences 3–5 s apart instead of 15 s).

Use it only when the application does not need the JetStream data to move (caches, events that are consumed at once), or add a migration step of your own (for example a stream mirror or source from blue to green). Deprecate or pause cron services in one colour if double runs matter.

### Joined cluster

Green uses the same NATS cluster name and route credentials, and its route list includes every blue and green pod. Blue's servers learn green's routes from the cluster, so blue needs no change.

{% hint style="warning" %}
**Joining moves work, not the Service flip.** As soon as green's servers join, green's instances join the same queue groups, fetch from the same durable consumers and receive cron occurrences. In the test, green answered 20 of 138 `hello` calls before the Service was flipped. The flip only moves HTTP ingress. Treat the overlap like a rolling update: both versions must be compatible.
{% endhint %}

**Do not delete blue to finish the cutover.** The JetStream meta group then contains all six servers, and three are not a quorum: deleting blue made JetStream unavailable on green (30.6 s of `502` until blue came back). Blue's servers also still hold every stream replica, because joining does not move data.

#### Prerequisites

1. **A system account user on every server.** Removing a server from the meta group (`nats server raft peer-remove`) is only allowed in the system account. Enable [`WithNATSSystemAccount`](../api/framework.md#withnatssystemaccount) on both colours (the example reads the password from `NATS_SYSTEM_PASSWORD`), and port-forward to a pod of the colour that stays:

   ```bash
   kubectl -n mono-demo port-forward pod/mono-green-0 14223:4222 &
   nats -s nats://127.0.0.1:14223 --user admin --password "$NATS_SYSTEM_PASSWORD" server report jetstream
   ```

   The report must list every server of both colours. For an application without the option, a temporary `nats-server` with JetStream disabled (otherwise it becomes a meta peer itself), the same cluster name and route credentials, a `$SYS` user (`accounts { "$SYS": { users: [ { user: admin, password: "..." } ] } }`, `system_account: "$SYS"`) and routes to every pod works as well.

2. **Every stream replicated across all old servers.** The runbook takes old servers offline one by one. A stream with 1 replica on an old server becomes unreachable once that server is offline, and one with 2 replicas loses its quorum when the second goes. List **every** stream (`nats stream ls`, including KV and object store buckets) and raise them to 3 replicas first.

#### Cutover runbook (blue to green)

With the Service flipped to green and all six servers in the meta group, repeat for each blue server, highest ordinal first (`mono-cluster-2`, then `-1`, then `-0`):

```bash
# 1. Take the server offline (the StatefulSet removes the highest ordinal)
kubectl -n mono-demo scale sts mono-cluster --replicas=2
kubectl -n mono-demo wait --for=delete pod/mono-cluster-2

# 2. Remove it from every stream; consumers follow their stream. The replacement
#    can only be an online server that is not yet a member: a green one.
for s in $(nats stream ls -n); do nats stream cluster peer-remove "$s" mono-cluster-2 -f; done

# 3. Wait until every stream and consumer lists only current replicas
nats stream info GREETER_VISITS
nats consumer info GREETER_VISITS greeter-visits

# 4. Remove it from the meta group (system account)
nats server raft peer-remove mono-cluster-2 -f --user admin --password "$NATS_SYSTEM_PASSWORD"
```

After the last blue server, the meta group and every stream consist of green servers only, with their history intact. The test moved three servers in about 35 s with no failed request.

**Take the server offline before removing it from the streams.** Removing an online server lets nats-server choose any non-member as the replacement, including another old server: in a test with two servers per colour, the replacement replica switched between the two old servers six times in a row. Once the server is offline, only the new colour's servers can be chosen.

#### Switching back

The reverse cutover is the same runbook with the colours swapped. One wait applies: a server removed from the meta group can rejoin under the same name only after nats-server's peer removal timeout of **5 minutes** (`peerRemoveTimeout` in nats-server v2.14). Before that, blue pods start, serve and pass their probes, but they hold no JetStream data and do not count toward the quorum. Wait 5 minutes after the last removal, scale blue back up, check that `nats server report jetstream` lists the blue servers, then run the runbook for each green server. The test's reverse cutover had no failed request.

## Delivery During Deployments

Every strategy here keeps the framework's guarantees: stream consumer and cron messages are delivered **at least once**, and durable consumers keep their position across restarts and cutovers (no replay was observed in any run). Plan for these effects:

- **Redelivery after the ack wait.** A message handled but not acknowledged before its pod stopped, or whose acknowledgement was lost while a consumer leader moved, is delivered again after `AckWait`. Handlers must be idempotent. In one experiment that moved consumer leaders several times in quick succession, 29 messages were delivered a second time; the cutover runbook runs had none.
- **A publish error doesn't mean the message wasn't stored.** A publish that timed out while the cluster had no quorum was stored and handled once the quorum returned (seen on 2 servers). A caller that retries can store it twice. Publish with a message ID (`helper.WithMsgID`, see [Typed Service Contracts](../api/container.md#typed-service-contracts)) and retry with the same ID: the stream stores the message once.

## Methodology and Limits

- **Setup:** the Kubernetes cluster example on Kubernetes 1.37 with 3 worker-capable nodes of 1 GiB each, nats-server v2.14.10, mono v0.2.0. Load was 5 requests per second through the ClusterIP Service, generated inside the cluster.
- **Each scenario ran once.** The numbers show behaviour, not latency distributions.
- **The "new version" differed only in its greeting text** (to make mixed-version serving visible). No run upgraded the framework itself, so behaviour changes between framework versions (such as the consumer conversion of v0.2.0) are not covered here; read the [upgrading guide](../guides/upgrading.md) for those.
- **The bad release failed before its NATS server started.** A release that joins the cluster and then fails is not covered.
- **The 5-minute rejoin wait** was observed on a 2+2 server test and confirmed in the nats-server v2.14.10 source; the 3+3 reverse cutover waited it out.
- **The system account:** the measured cutovers used a separate admin server; a later cutover and reverse cutover with `WithNATSSystemAccount` on every pod and no admin server had no failed request either (609 and 685 requests). A config file with only a `$SYS` user and `system_account` was checked with a single server.

## Related Documentation

- [Clustering and Kubernetes guide](../guides/clustering.md): requirements, configuration, the baseline Kubernetes setup
- [Kubernetes cluster example](../../../examples/k8s-cluster/README.md): the manifests and application used for these tests
- [Upgrading guide](../guides/upgrading.md): framework version changes
