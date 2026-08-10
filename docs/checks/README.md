# Insights audit checks

This catalog documents 142 checks and their corresponding SQL macros. Operational
checks use range macros (`epoch_start`, `epoch_end`); optimization checks use
snapshot macros (`epoch_ts`, `range_start`). The SQL definitions are available in
[`queries/`](queries/).

## ACCOUNTS_001: Account Connection Limit

- -- Flags accounts where connections on any single server are at or above 90% of
- -- the configured limit. NATS enforces max_conn per server, so the worst
- -- per-server ratio is what matters; current_val/pct reflect the most saturated
- -- server, not the aggregate across servers.
- -- Severity: warning | Threshold: per-server connections >= 90% of limit
- -- Extra columns: current_val (BIGINT), max_val (BIGINT), pct (DOUBLE), unit (VARCHAR), server_name (VARCHAR)
- **Macro**: `audit.check_accounts_001(epoch_start, epoch_end, p_warn_percent := 90.0)`

## ACCOUNTS_002: Slow Consumers

- -- Flags accounts with new slow consumer events since the previous epoch, aggregated across servers.
- -- Severity: critical | Threshold: slow_consumers delta > 0
- **Macro**: `audit.check_accounts_002(epoch_start, epoch_end)`

## ACCOUNTS_003: Inactive JWT Import

- -- Detects JWT-sourced imports that are invalid (broken or never activated).
- -- Diagnoses root cause: missing activation token, expired token, token signed by rotated signing key, or source export not found.
- -- Severity: critical
- **Macro**: `audit.check_accounts_003(epoch_start, epoch_end)`

## ACCOUNTS_004: Orphaned Export

- -- Flags exports with no matching importer in any account.
- -- Severity: warning
- **Macro**: `audit.check_accounts_004(epoch_start, epoch_end)`

## ACCOUNTS_005: No Subscription Interest

- -- Finds active imports where no client in the importing account subscribes to the imported subject.
- -- Severity: info
- -- The anti-join evaluates imports x distinct-subjects-per-account pairs (~430k
- -- on prod-shaped data), which made this check ~80% of per-epoch check
- -- materialization (INS-138). Instead of calling audit.subjects_overlap per
- -- pair — re-splitting both subjects each time — each side is split once per
- -- row and the pair predicate inlines the macro's guard/short-circuit structure
- -- (see audit.subjects_overlap in 00_schema.sql) with a first-token
- -- compatibility conjunct pruning most pairs before the token-list comparison.
- **Macro**: `audit.check_accounts_005(epoch_start, epoch_end)`

## ACCOUNTS_006: Account Subscription Limit

- -- Flags accounts where the subscription gauge (account_stats.subs, summed
- -- across servers) is at or above 90% of the configured limit. NATS enforces
- -- max_subs against the account's subscription gauge, not the per-subscription
- -- detail rows the scraper collects.
- -- Severity: warning | Threshold: subscriptions >= 90% of limit
- -- Extra columns: current_val (BIGINT), max_val (BIGINT), pct (DOUBLE), unit (VARCHAR)
- **Macro**: `audit.check_accounts_006(epoch_start, epoch_end, p_warn_percent := 90.0)`

## CHANGE_001: Config Reload Detected

- -- Detects servers whose configuration was reloaded (config_load_time changed
- -- between consecutive epochs).
- -- Severity: info | Threshold: config_load_time changed
- **Macro**: `audit.check_change_001(epoch_start, epoch_end)`

## CHANGE_002: JetStream Domain Changed

- -- Detects servers whose JetStream domain value changed between consecutive epochs.
- -- Severity: warning | Threshold: js_domain changed
- **Macro**: `audit.check_change_002(epoch_start, epoch_end)`

## CHANGE_003: Account Added or Removed

- -- Detects accounts that appeared or disappeared between consecutive epochs.
- -- Severity: info | Threshold: account presence changed
- **Macro**: `audit.check_change_003(epoch_start, epoch_end)`

## CHANGE_004: Stream Configuration Changed

- -- Detects key stream config fields that changed between consecutive epochs.
- -- Compares num_replicas, retention_policy, max_msgs, max_bytes, max_age,
- -- and max_consumers between epoch pairs.
- -- Severity: info | Threshold: any monitored field changed
- **Macro**: `audit.check_change_004(epoch_start, epoch_end)`

## CLUSTER_001: Memory Usage Outlier

- -- Flags servers whose memory usage exceeds 1.5 times the average of their cluster.
- -- Severity: warning | Threshold: memory > 1.5x cluster average
- -- Extra columns: memory_ratio (DOUBLE), cluster_name (VARCHAR)
- **Macro**: `audit.check_cluster_001(epoch_start, epoch_end, p_multiplier := 1.5)`

## CLUSTER_003: High HA Assets

- -- Flags servers with 1000 or more highly-available JetStream assets.
- -- Severity: warning | Threshold: ha_assets >= 1000
- -- Extra columns: ha_assets (BIGINT)
- **Macro**: `audit.check_cluster_003(epoch_start, epoch_end, p_warn_count := 1000)`

## CLUSTER_004: Cluster Name Whitespace

- -- Flags servers whose cluster name contains whitespace characters.
- -- Severity: warning | Threshold: whitespace in cluster name
- -- Extra columns: cluster_name (VARCHAR)
- **Macro**: `audit.check_cluster_004(epoch_start, epoch_end)`

## CLUSTER_005: Route Count Low

- -- Flags servers with fewer routes than expected for their cluster.
- -- Expected routes = number of other servers in same cluster.
- -- Severity: warning | Threshold: routes < expected
- -- Extra columns: routes (BIGINT), expected (BIGINT), member_count (BIGINT)
- -- DEVIATION: bypasses the hx.servers view because its server_opts join fans
- -- out rows per (pk, epoch), inflating count(*)-based membership and
- -- duplicating output rows. Membership is counted over DISTINCT server pks,
- -- and cluster names are trimmed (empty -> NULL) so whitespace variants
- -- collapse into one cluster.
- **Macro**: `audit.check_cluster_005(epoch_start, epoch_end)`

## CLUSTER_006: Connection Count Change

- -- Flags servers where the number of connected clients changed dramatically
- -- between epochs. Diffs the `connections` gauge (current active clients),
- -- not the `total_connections` monotonic counter, so the delta reflects a
- -- change in connected clients and can legitimately be negative.
- -- Severity: warning | Threshold: abs(connections delta) > 500
- -- Extra columns: delta (BIGINT)
- **Macro**: `audit.check_cluster_006(epoch_start, epoch_end, p_max_delta := 500)`

## CLUSTER_007: Gateway Disconnection

- -- Flags servers that lost connectivity to a remote cluster since the previous
- -- epoch. Connectivity is keyed on (server_pk, normalized remote cluster) — the
- -- logical link — NOT on gateway_pk, which is a per-connection identity. A
- -- gateway reconnect mints a new id (hence a new pk), so keying on gateway_pk
- -- would fire "disconnection" on reconnect churn even when a replacement gateway
- -- to the same remote cluster is present in the very next epoch. Loss is the
- -- anti-join of the previous epoch's (server, remote cluster) set against the
- -- current epoch's set. Remote-cluster names are normalized (lower + trim, empty
- -- -> NULL) per the CLUSTER_008 idiom so whitespace/case variants collapse.
- -- Severity: critical | Threshold: (server, remote cluster) link present at prev epoch but missing at current
- -- Extra columns: remote_cluster (VARCHAR)
- **Macro**: `audit.check_cluster_007(epoch_start, epoch_end)`

## CLUSTER_008: Gateway Config Mismatch

- -- Flags servers whose set of gateway connections differs from the cluster majority.
- -- Remote-cluster names are normalized (lower + trim, empty -> NULL) so
- -- whitespace/case variants of the same cluster collapse to one set element.
- -- The majority set must be held by a strict majority of the cluster's
- -- gateway-bearing servers, and the cluster must have at least 3 such servers
- -- ("majority" is ill-defined below that); the ROW_NUMBER tiebreaker
- -- (cnt DESC, gw_set) keeps selection deterministic.
- -- Severity: warning | Threshold: gateway set differs from strict-majority set in cluster (>= 3 servers)
- -- Extra columns: server_gateways (VARCHAR), majority_gateways (VARCHAR)
- **Macro**: `audit.check_cluster_008(epoch_start, epoch_end)`

## CONN_001: High Client RTT

- -- Flags client connections with round-trip time exceeding 100 ms.
- -- Severity: warning | Threshold: rtt > 100 ms
- **Macro**: `audit.check_conn_001(epoch_start, epoch_end, p_rtt_ms := 100.0)`

## CONN_002: Client Pending Pressure

- -- Flags client connections with more than 1 MiB of pending bytes.
- -- Severity: warning | Threshold: pending_bytes > 1 MiB
- **Macro**: `audit.check_conn_002(epoch_start, epoch_end, p_pending_mib := 1.0)`

## CONN_003: Connection Stopped

- -- Flags connections that disconnected with a non-empty reason.
- -- Severity: info | Threshold: stop_time set with non-empty reason
- **Macro**: `audit.check_conn_003(epoch_start, epoch_end)`

## CONSUMER_001: Consumer Replica Offline

- -- Flags consumer replicas that are reported as offline.
- -- Severity: critical | Threshold: is_offline = true
- **Macro**: `audit.check_consumer_001(epoch_start, epoch_end)`

## CONSUMER_002: Consumer Replica Lag

- -- Flags online consumer replicas lagging by more than 1000 operations behind the leader.
- -- Offline replicas are excluded: their lag gauge is stale and CONSUMER_001
- -- already reports the outage.
- -- Severity: warning | Threshold: lag > 1000 operations
- **Macro**: `audit.check_consumer_002(epoch_start, epoch_end, p_max_lag := 1000)`

## CONSUMER_003: Consumer Quorum Lost

- -- Flags replicated consumers where enough replicas are offline to lose quorum.
- -- Severity: critical | Threshold: offline replicas * 2 > num_replicas (R > 1 only)
- -- Limitation: if the leader itself is offline, no peer rows are emitted for that
- -- epoch, so this check will not fire even when quorum is actually lost.
- **Macro**: `audit.check_consumer_003(epoch_start, epoch_end)`

## CONSUMER_004: Consumer Delivered Below Stream First Sequence

- -- Flags consumers whose last delivered position is below the stream's first sequence after a purge or truncation.
- -- Severity: critical | Threshold: delivered_stream_seq > 0 AND delivered_stream_seq < stream first_seq
- -- Extra columns: delivered_seq (BIGINT), stream_first_seq (BIGINT)
- **Macro**: `audit.check_consumer_004(epoch_start, epoch_end)`

## CONSUMER_005: Consumer Sequence Ahead of Stream Sequence

- -- Flags consumers whose delivered position is ahead of the stream's last sequence.
- -- Severity: critical | Threshold: delivered_stream_seq > stream last_seq
- -- Extra columns: delivered_seq (BIGINT), stream_last_seq (BIGINT)
- **Macro**: `audit.check_consumer_005(epoch_start, epoch_end)`

## CONSUMER_006: Outstanding Ack Critical

- -- Flags consumers where num_ack_pending is at or above the operator-defined threshold.
- -- Matches the upstream NATS monitor contract (jsm.go consumerCheckOutstandingAck):
- -- fires when num_ack_pending >= threshold; threshold values <= 0 disable the check.
- -- Severity: critical | Threshold: metadata io.nats.monitor.outstanding-ack-critical
- **Macro**: `audit.check_consumer_006(epoch_start, epoch_end)`

## CONSUMER_007: Waiting Critical

- -- Flags consumers where num_waiting is at or above the operator-defined threshold.
- -- Matches the upstream NATS monitor contract (jsm.go consumerCheckWaiting):
- -- fires when num_waiting >= threshold; threshold values <= 0 disable the check.
- -- Severity: critical | Threshold: metadata io.nats.monitor.waiting-critical
- **Macro**: `audit.check_consumer_007(epoch_start, epoch_end)`

## CONSUMER_008: Unprocessed Critical

- -- Flags consumers where num_pending is at or above the operator-defined threshold.
- -- Matches the upstream NATS monitor contract (jsm.go consumerCheckUnprocessed):
- -- fires when num_pending >= threshold; threshold values <= 0 disable the check.
- -- Severity: critical | Threshold: metadata io.nats.monitor.unprocessed-critical
- **Macro**: `audit.check_consumer_008(epoch_start, epoch_end)`

## CONSUMER_009: Last Delivery Critical

- -- Flags consumers where time since last delivery is at or above the operator-defined threshold.
- -- Matches the upstream NATS monitor contract (jsm.go consumerCheckLastDelivery):
- -- fires when elapsed >= threshold; a threshold that parses to <= 0 disables the check.
- -- Severity: critical | Threshold: metadata io.nats.monitor.last-delivery-critical (Go duration)
- **Macro**: `audit.check_consumer_009(epoch_start, epoch_end)`

## CONSUMER_010: Last Ack Critical

- -- Flags consumers where time since last ack is at or above the operator-defined threshold.
- -- Matches the upstream NATS monitor contract (jsm.go consumerCheckLastAck):
- -- fires when elapsed >= threshold; a threshold that parses to <= 0 disables the check.
- -- Severity: critical | Threshold: metadata io.nats.monitor.last-ack-critical (Go duration)
- **Macro**: `audit.check_consumer_010(epoch_start, epoch_end)`

## CONSUMER_011: Redelivery Critical

- -- Flags consumers where num_redelivered is at or above the operator-defined threshold.
- -- Matches the upstream NATS monitor contract (jsm.go consumerCheckRedelivery):
- -- fires when num_redelivered >= threshold; threshold values <= 0 disable the check.
- -- Severity: critical | Threshold: metadata io.nats.monitor.redelivery-critical
- **Macro**: `audit.check_consumer_011(epoch_start, epoch_end)`

## CONSUMER_012: Pinned Consumer Policy Mismatch

- -- Flags consumers with monitor.pinned metadata that are not using the pinned_client priority policy.
- -- Severity: critical | Threshold: metadata io.nats.monitor.pinned = 'true' but priority_policy != 'pinned_client'
- **Macro**: `audit.check_consumer_012(epoch_start, epoch_end)`

## JETSTREAM_001: Stream Replica Lag

- -- Flags stream replicas whose last sequence number is more than 10% behind the leader.
- -- Severity: warning | Threshold: replica > 10% behind leader last_seq
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view)
- -- and hx.server_ident for peer server name (only peer_server_pk is on view).
- -- NOTE: Self-reported rows have peer_server_pk = 0 (server_pk is the replica server).
- -- Leader-reported peer rows have peer_server_pk != 0 (the follower hosting the replica).
- -- Only self-reported follower rows (peer_server_pk = 0, is_leader = false) have actual last_seq.
- -- Followers reporting last_seq = 0 are excluded: they have no initialized state yet
- -- (e.g. mid snapshot sync) and would otherwise compute as 100% lag.
- -- Extra columns: replica_server_pk (BIGINT), replica_server (VARCHAR), replica_seq (BIGINT), leader_seq (BIGINT), lag_percent (DOUBLE)
- **Macro**: `audit.check_jetstream_001(epoch_start, epoch_end, p_lag_percent := 10.0)`

## JETSTREAM_002: High Subject Cardinality

- -- Flags streams (leader only) with one million or more unique subjects.
- -- Severity: warning | Threshold: num_subjects >= 1,000,000
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- -- Extra columns: num_subjects (BIGINT)
- **Macro**: `audit.check_jetstream_002(epoch_start, epoch_end, p_max_subjects := 1000000)`

## JETSTREAM_003: Stream Message Limit

- -- Flags streams (leader only) where message count is at or above 90% of the limit.
- -- Severity: warning | Threshold: messages >= 90% of max_msgs
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- -- Extra columns: current_val (BIGINT), max_val (BIGINT), pct (DOUBLE)
- **Macro**: `audit.check_jetstream_003(epoch_start, epoch_end, p_warn_percent := 90.0)`

## JETSTREAM_004: JS API Request Rate High

- -- Flags servers where the JetStream API request rate exceeds the threshold.
- -- Severity: warning | Threshold: per-server request rate >= 50 req/s
- -- Counters are monotonic per server; deltas are clamped with GREATEST(..., 0)
- -- to absorb counter resets and divided by the actual epoch interval for a
- -- per-second rate (same structure as JETSTREAM_009).
- -- Extra columns: rate_rps (DOUBLE), delta_requests (BIGINT), error_delta (BIGINT)
- **Macro**: `audit.check_jetstream_004(epoch_start, epoch_end, p_max_rps := 50.0)`

## JETSTREAM_005: JS API Pending High

- -- Flags servers where JetStream API inflight requests exceed threshold.
- -- Severity: warning | Threshold: js_api_inflight > 1000
- -- Extra columns: js_api_inflight (BIGINT)
- **Macro**: `audit.check_jetstream_005(epoch_start, epoch_end, p_max_pending := 1000)`

## JETSTREAM_006: Consumer Count Change

- -- Flags when the consumer replica count change between epochs exceeds the threshold.
- -- Severity: warning | Threshold: absolute delta > 5000
- -- Extra columns: prev_count (BIGINT), current_count (BIGINT)
- **Macro**: `audit.check_jetstream_006(epoch_start, epoch_end, p_max_delta := 5000)`

## JETSTREAM_007: JetStream Memory Utilization Critical

- -- Flags servers where JetStream memory usage exceeds the critical threshold.
- -- Severity: critical | Threshold: memory >= 95% of reserved
- -- Extra columns: pct (DOUBLE)
- **Macro**: `audit.check_jetstream_007(epoch_start, epoch_end, p_crit_percent := 95.0)`

## JETSTREAM_008: Stream Quorum Lost

- -- Flags replicated streams where enough replicas are offline to lose quorum.
- -- Severity: critical | Threshold: offline replicas * 2 > num_replicas (R > 1 only)
- -- Extra columns: offline_count (BIGINT), num_replicas (BIGINT), quorum_needed (BIGINT)
- **Macro**: `audit.check_jetstream_008(epoch_start, epoch_end)`

## JETSTREAM_009: JS API Error Rate High

- -- Flags servers where JetStream API errors exceed a percentage of total requests.
- -- Severity: warning | Threshold: error_delta / total_delta > p_error_percent AND total_delta >= p_min_requests
- -- Extra columns: error_delta (BIGINT), total_delta (BIGINT), error_pct (DOUBLE)
- **Macro**: `audit.check_jetstream_009(epoch_start, epoch_end, p_error_percent := 1.0, p_min_requests := 100)`

## JETSTREAM_010: Stream Byte Limit

- -- Flags streams (leader only) where byte usage is at or above 90% of the limit.
- -- Severity: warning | Threshold: bytes >= 90% of max_bytes
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- -- Extra columns: current_val (BIGINT), max_val (BIGINT), pct (DOUBLE)
- **Macro**: `audit.check_jetstream_010(epoch_start, epoch_end, p_warn_percent := 90.0)`

## JETSTREAM_011: Stream Consumer Limit

- -- Flags streams (leader only) where consumer count is at or above 90% of the limit.
- -- Severity: warning | Threshold: num_consumers >= 90% of max_consumers
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- -- Extra columns: current_val (BIGINT), max_val (BIGINT), pct (DOUBLE)
- **Macro**: `audit.check_jetstream_011(epoch_start, epoch_end, p_warn_percent := 90.0)`

## JETSTREAM_013: Stream Subject/Message Count Inconsistency

- -- Flags streams where num_subjects > msgs — an invariant violation indicating filestore corruption.
- -- Severity: warning | Threshold: num_subjects > msgs (msgs > 0, leader only)
- -- Extra columns: num_subjects (BIGINT), msgs (BIGINT)
- **Macro**: `audit.check_jetstream_013(epoch_start, epoch_end)`

## JETSTREAM_014: Stream Replica Message Count Divergence

- -- Flags replicated streams where all replicas report current but message counts diverge significantly.
- -- Severity: critical | Threshold: divergence > 5% AND > 1000 messages absolute
- -- Extra columns: min_msgs (BIGINT), max_msgs (BIGINT), divergence_pct (DOUBLE)
- **Macro**: `audit.check_jetstream_014(epoch_start, epoch_end, p_divergence_percent := 5.0, p_min_divergence := 1000)`

## JETSTREAM_015: Mirror Last Seen Staleness

- -- Flags mirror streams where the mirror consumer has stalled — zero lag but no activity
- -- while the source stream continues receiving messages.
- -- Severity: warning | Threshold: mirror_lag = 0 AND mirror_active > p_stale_minutes
- -- NOTE: Joins source stream by mirror_name + account to verify source is active.
- -- False positive risk: if source stream cannot be resolved, the check is skipped for that mirror.
- -- Extra columns: mirror_active_minutes (DOUBLE), mirror_name (VARCHAR)
- **Macro**: `audit.check_jetstream_015(epoch_start, epoch_end, p_stale_minutes := 5.0)`

## JETSTREAM_017: Mirror Lag Critical

- -- Flags mirror streams where mirror lag exceeds the operator-defined threshold.
- -- Severity: critical | Threshold: metadata io.nats.monitor.lag-critical
- **Macro**: `audit.check_jetstream_017(epoch_start, epoch_end)`

## JETSTREAM_018: Mirror Seen Critical

- -- Flags mirror streams where mirror active time exceeds the operator-defined threshold.
- -- Severity: critical | Threshold: metadata io.nats.monitor.seen-critical (Go duration)
- **Macro**: `audit.check_jetstream_018(epoch_start, epoch_end)`

## JETSTREAM_019: Min Sources

- -- Flags streams where the source count is below the operator-defined minimum.
- -- Severity: critical | Threshold: metadata io.nats.monitor.min-sources
- **Macro**: `audit.check_jetstream_019(epoch_start, epoch_end)`

## JETSTREAM_020: Max Sources

- -- Flags streams where the source count exceeds the operator-defined maximum.
- -- Severity: critical | Threshold: metadata io.nats.monitor.max-sources
- **Macro**: `audit.check_jetstream_020(epoch_start, epoch_end)`

## JETSTREAM_021: Peer Expect

- -- Flags streams where the actual peer count does not match the operator-defined expected count.
- -- Severity: critical | Threshold: metadata io.nats.monitor.peer-expect
- **Macro**: `audit.check_jetstream_021(epoch_start, epoch_end)`

## JETSTREAM_022: Peer Lag Critical

- -- Flags stream replicas where lag exceeds the operator-defined threshold.
- -- Severity: critical | Threshold: metadata io.nats.monitor.peer-lag-critical
- **Macro**: `audit.check_jetstream_022(epoch_start, epoch_end)`

## JETSTREAM_023: Peer Seen Critical

- -- Flags stream replicas where active time exceeds the operator-defined threshold.
- -- Severity: critical | Threshold: metadata io.nats.monitor.peer-seen-critical (Go duration)
- **Macro**: `audit.check_jetstream_023(epoch_start, epoch_end)`

## JETSTREAM_024: Message Count Threshold

- -- Flags streams where message count exceeds operator-defined warn or critical thresholds.
- -- Directional: if critical > warn, checks for "too many"; if critical < warn, checks for "too few".
- -- DynamicSeverity: severity = 1 (warning) if only warn threshold exceeded, 2 (critical) if critical exceeded.
- **Macro**: `audit.check_jetstream_024(epoch_start, epoch_end)`

## JETSTREAM_025: Subject Count Threshold

- -- Flags streams where subject count exceeds operator-defined warn or critical thresholds.
- -- Directional: if critical > warn, checks for "too many"; if critical < warn, checks for "too few".
- -- DynamicSeverity: severity = 1 (warning) if only warn threshold exceeded, 2 (critical) if critical exceeded.
- **Macro**: `audit.check_jetstream_025(epoch_start, epoch_end)`

## LEAF_001: Leafnode Name Whitespace

- -- Flags leafnode connections whose remote server name contains whitespace characters.
- -- Severity: warning | Threshold: whitespace in leaf name
- -- Extra columns: leaf_name (VARCHAR)
- **Macro**: `audit.check_leaf_001(epoch_start, epoch_end)`

## LEAF_002: High Leaf RTT

- -- Flags leafnode connections with round-trip time exceeding the threshold.
- -- Severity: warning | Threshold: rtt > p_rtt_ms milliseconds
- -- Extra columns: rtt_ms (DOUBLE)
- **Macro**: `audit.check_leaf_002(epoch_start, epoch_end, p_rtt_ms := 100.0)`

## LEAF_003: Leafnode Subscription Count High

- -- Flags leafnode connections carrying a large number of subscriptions.
- -- Severity: dynamic (warning > warn_subs, critical > crit_subs) | Threshold: num_subs > p_warn_subs
- -- Extra columns: num_subs (BIGINT)
- **Macro**: `audit.check_leaf_003(epoch_start, epoch_end, p_warn_subs := 20000, p_crit_subs := 50000)`

## META_001: Offline Replica

- -- Flags meta cluster replicas that are reported as offline.
- -- Severity: critical | Threshold: meta peer offline per authoritative consensus
- -- DEVIATION: Queries hx.meta_cluster_stats directly and joins hx.server_ident
- -- because there is no hx view for meta_cluster_stats. The table is a
- -- (reporter server) x (peer) fan-out, so a peer marked offline by a single
- -- dissenting reporter (e.g. during an asymmetric partition, while the peer
- -- self-reports online) must not be flagged; collapse to one authoritative row
- -- per peer with the same leader-preference, online-wins dedup as META_006 and
- -- audit.context_meta_cluster_peers. The reporter extras come from the
- -- surviving deduped row.
- -- Extra columns: reporter_pk (BIGINT), reporter_name (VARCHAR)
- **Macro**: `audit.check_meta_001(epoch_start, epoch_end)`

## META_002: Leader Disagreement

- -- Flags when multiple servers report themselves as the meta cluster leader.
- -- Severity: critical | Threshold: more than 1 server claims meta leader
- -- DEVIATION: Queries hx.meta_cluster_stats directly (no hx view exists).
- -- The leader's self-report uses peer_server_pk=0 while followers report the
- -- leader as peer_server_pk=<leaderPK>; coalesce so both forms resolve to the
- -- same server PK.
- -- Extra columns: leader_count (BIGINT)
- **Macro**: `audit.check_meta_002(epoch_start, epoch_end)`

## META_003: Meta Leader Flapping

- -- Flags when the meta cluster leader has changed more than the allowed number
- -- of times within the rolling time window.
- -- Severity: warning | Threshold: leader changes > 1 within 10 minutes
- -- DEVIATION: Queries hx.meta_cluster_stats directly (no hx view exists).
- -- Leader identity is resolved per META_002's pattern: the leader's self-report
- -- uses peer_server_pk=0 while followers report the leader as
- -- peer_server_pk=<leaderPK>, so coalesce both forms to one PK. One
- -- authoritative leader is picked per epoch (most-reported, ties broken by
- -- lowest PK) so a same-instant multi-leader disagreement (META_002's domain)
- -- is NOT counted as a change; changes are transitions between consecutive
- -- epochs' authoritative leaders via LAG().
- -- Extra columns: changes (BIGINT)
- **Macro**: `audit.check_meta_003(epoch_start, epoch_end, p_max_changes := 1, p_window_minutes := 10)`

## META_004: Meta Snapshot Slow

- -- Flags when meta cluster snapshot duration exceeds threshold.
- -- Severity: warning at 5s, critical at 30s | Threshold: snapshot_duration_ns
- -- Extra columns: snapshot_seconds (DOUBLE)
- **Macro**: `audit.check_meta_004(epoch_start, epoch_end, p_warn_seconds := 5.0, p_crit_seconds := 30.0)`

## META_005: Meta State Growth

- -- Flags when total JetStream asset replicas exceeds threshold.
- -- Severity: warning | Threshold: total stream + consumer replicas > 5000
- -- Extra columns: total_replicas (BIGINT)
- **Macro**: `audit.check_meta_005(epoch_start, epoch_end, p_max_assets := 5000)`

## META_006: Meta Quorum Lost

- -- Flags when enough meta cluster peers are offline to lose quorum.
- -- Severity: critical | Threshold: offline peers * 2 > cluster_size
- -- DEVIATION: Queries hx.meta_cluster_stats directly (no hx view exists).
- -- The table is a (reporter server) x (peer) fan-out, so a peer marked offline
- -- by a single dissenting reporter must not be counted as offline; collapse to
- -- one authoritative row per peer with the same leader-preference, online-wins
- -- dedup as audit.context_meta_cluster_peers so the check and context view agree.
- -- Extra columns: offline_count (BIGINT), cluster_size (BIGINT), quorum_needed (BIGINT)
- **Macro**: `audit.check_meta_006(epoch_start, epoch_end)`

## META_007: Even Cluster Size

- -- Flags when the meta cluster has an even number of peers, risking split-brain.
- -- Severity: warning | Threshold: cluster_size is even and > 1
- -- Extra columns: cluster_size (BIGINT)
- **Macro**: `audit.check_meta_007(epoch_start, epoch_end)`

## META_008: Meta Pending High

- -- Flags when the meta cluster leader has a high number of pending Raft operations.
- -- Severity: warning | Threshold: pending > 500
- -- DEVIATION: Queries hx.meta_cluster_stats directly (no hx view exists) and
- -- joins hx.server_ident on the RESOLVED leader PK. The leader's self-report
- -- uses peer_server_pk=0 while followers report the leader as
- -- peer_server_pk=<leaderPK>; coalesce so both forms resolve to the same leader
- -- PK, then dedup to one row per epoch so a high-pending leader is reported
- -- once, attributed to the leader (not a reporting follower).
- -- Extra columns: pending (BIGINT)
- **Macro**: `audit.check_meta_008(epoch_start, epoch_end, p_max_pending := 500)`

## META_009: Meta Cluster Size Decreased

- -- Flags when the meta cluster's distinct peer count has decreased between
- -- consecutive epochs.
- -- Severity: critical | Threshold: current peer count < previous peer count
- -- DEVIATION: Queries hx.meta_cluster_stats directly (no hx view exists).
- -- The size is measured as the distinct peer count per epoch — resolving the
- -- leader's peer_server_pk=0 self-report per META_002's pattern — NOT the
- -- self-reported cluster_size scalar, which is a gauge that can blip
- -- anomalously (observed: 7 at one epoch while the distinct peer set stayed
- -- at 3) and drive false fires when the blip returns to baseline.
- -- Extra columns: prev_size (BIGINT), current_size (BIGINT)
- **Macro**: `audit.check_meta_009(epoch_start, epoch_end)`

## OPT_ACCT_001: Account Storage Quota Approaching Limit

- -- Flags accounts where JetStream storage reservations approach the configured quota.
- -- Approximation: sums stream reservations (max_bytes * num_replicas), not actual bytes.
- -- Streams with max_bytes = -1 (unlimited) are excluded from the sum.
- -- Severity: warning | Threshold: reservations > p_warn_percent% of js_disk_storage
- -- Extra columns: reserved_bytes (BIGINT), quota_bytes (BIGINT), pct (DOUBLE)
- **Macro**: `audit.check_opt_acct_001(epoch_ts, range_start, p_warn_percent := 85.0)`

## OPT_ACCT_002: Excessive JWT Size

- -- Flags accounts with unusually large JWT claims.
- -- Severity: warning | Threshold: length(claims) > p_max_size_kib KiB
- -- Extra columns: claims_size_kib (DOUBLE)
- **Macro**: `audit.check_opt_acct_002(epoch_ts, range_start, p_max_size_kib := 64.0)`

## OPT_BALANCE_001: Uneven Leader Distribution

- -- Flags servers hosting disproportionately many stream and consumer leaders
- -- compared to their cluster average.
- -- Severity: info | Threshold: leaders > 1.5x cluster average (min 3 servers)
- **Macro**: `audit.check_opt_balance_001(epoch_ts, range_start)`

## OPT_BALANCE_002: Connection Hotspot

- -- Flags servers with more than double the cluster average connections and
- -- at least 100 connections.
- -- Severity: info | Threshold: connections > 2x cluster average (min 100 absolute)
- **Macro**: `audit.check_opt_balance_002(epoch_ts, range_start)`

## OPT_BALANCE_003: Subscription Hotspot

- -- Flags servers with more than double the cluster average subscriptions and
- -- at least 100 subscriptions.
- -- Severity: info | Threshold: subscriptions > 2x cluster average (min 100 absolute)
- **Macro**: `audit.check_opt_balance_003(epoch_ts, range_start)`

## OPT_BALANCE_004: Stream Replica Count Imbalance

- -- Flags servers hosting disproportionately many stream replicas, causing uneven
- -- storage I/O and memory pressure.
- -- Unlike OPT_BALANCE_001 (leaders only), this detects overall replica placement skew.
- -- Severity: info | Threshold: replicas > 1.5x cluster average (min 3 servers, min 10 replicas)
- **Macro**: `audit.check_opt_balance_004(epoch_ts, range_start)`

## OPT_BALANCE_005: JetStream Storage Skew

- -- Flags servers whose JetStream disk storage exceeds double the cluster average,
- -- indicating uneven data distribution.
- -- Severity: info | Threshold: storage > 2x cluster average (min 1 GiB absolute)
- **Macro**: `audit.check_opt_balance_005(epoch_ts, range_start)`

## OPT_BALANCE_006: Account Connection Concentration

- -- Flags servers hosting more than 70% of an account's connections within a cluster,
- -- indicating single-point-of-failure risk.
- -- Severity: info | Threshold: > 70% of account connections on one server (min 3 servers, min 10 connections)
- **Macro**: `audit.check_opt_balance_006(epoch_ts, range_start)`

## OPT_BALANCE_007: Stream-Consumer Leader Co-location

- -- Flags streams where the stream leader's server also hosts a disproportionate share
- -- of replicated consumer leaders, concentrating I/O on a single node. R1 consumers
- -- are excluded: their leader is pinned to the single replica's server, so a
- -- step-down cannot move them and they would inflate the co-location ratio.
- -- Severity: info | Threshold: > 50% of replicated consumer leaders on stream leader's server (min 3 consumers)
- -- Extra columns: consumer_leaders_on_server (BIGINT), total_consumer_leaders (BIGINT), pct (DOUBLE)
- **Macro**: `audit.check_opt_balance_007(epoch_ts, range_start, p_consumer_leader_pct := 50.0)`

## OPT_BALANCE_008: JetStream Storage Saturation with Skew

- -- Flags servers near JetStream storage capacity where the cluster also exhibits
- -- significant storage skew between nodes.
- -- Severity: warning | Threshold: pct >= saturation_pct AND (cluster_max - cluster_min) > skew_pp
- -- Extra columns: pct (DOUBLE), cluster_min_pct (DOUBLE), cluster_max_pct (DOUBLE), cluster_name (VARCHAR)
- **Macro**: `audit.check_opt_balance_008(epoch_ts, range_start, p_saturation_pct := 90.0, p_skew_pp := 30.0)`

## OPT_COST_001: Over-Replicated Inactive Stream

- -- Flags replicated (R3+) streams that received no new messages across the selected time range.
- -- Severity: info | Threshold: R3+ stream with no new messages across time range
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- **Macro**: `audit.check_opt_cost_001(epoch_ts, range_start)`

## OPT_COST_002: Memory Storage Large Stream

- -- Flags memory-backed streams using more than 100 MiB (leader only).
- -- Severity: info | Threshold: memory-backed stream > 100 MiB
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- **Macro**: `audit.check_opt_cost_002(epoch_ts, range_start, p_max_memory_mib := 100.0)`

## OPT_COST_003: Wasted JetStream Memory Reservation

- -- Flags servers where JetStream memory usage is below 20% of reserved capacity.
- -- Severity: info | Threshold: memory usage < 20% of reserved
- **Macro**: `audit.check_opt_cost_003(epoch_ts, range_start, p_min_utilization_percent := 20.0)`

## OPT_COST_004: Uncompressed Large Stream

- -- Flags file-backed streams exceeding 1 GiB that have no compression enabled (leader only).
- -- Severity: info | Threshold: file stream > 1 GiB with compression disabled
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- **Macro**: `audit.check_opt_cost_004(epoch_ts, range_start, p_max_uncompressed_gib := 1.0)`

## OPT_COST_005: Wasted JetStream Storage Reservation

- -- Flags servers where JetStream storage usage is below 20% of reserved capacity.
- -- Severity: info | Threshold: storage usage < 20% of reserved
- **Macro**: `audit.check_opt_cost_005(epoch_ts, range_start, p_min_utilization_percent := 20.0)`

## OPT_IDLE_001: Underutilized Server

- -- Flags servers that remained nearly idle across the selected time range.
- -- Severity: info | Threshold: max CPU < 5% AND max connections < 10 across range (min 5 samples, reporting at current epoch)
- **Macro**: `audit.check_opt_idle_001(epoch_ts, range_start, p_max_cpu_percent := 5.0, p_max_connections := 10)`

## OPT_IDLE_002: Inactive Stream

- -- Flags unsealed streams that received no new messages across the selected time range.
- -- Severity: info | Threshold: last_seq unchanged across time range (excludes sealed)
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- **Macro**: `audit.check_opt_idle_002(epoch_ts, range_start)`

## OPT_IDLE_003: Inactive Consumer

- -- Flags consumers that made no delivery progress across the selected time range.
- -- Severity: info | Threshold: delivered_stream_seq unchanged between earliest and
- -- latest leader samples in the range (min 2 samples, reporting at current epoch)
- -- DEVIATION: Joins hx.stream_ident and hx.account_ident for entity name.
- **Macro**: `audit.check_opt_idle_003(epoch_ts, range_start)`

## OPT_IDLE_004: Drained Consumer

- -- Flags consumers that are fully caught up with zero pending messages on a stream
- -- that is also inactive. May be safe to remove.
- -- Severity: info | Threshold: num_pending = 0 AND num_ack_pending = 0 at current epoch on inactive stream
- -- DEVIATION: Joins hx.stream_ident and hx.account_ident for entity name.
- **Macro**: `audit.check_opt_idle_004(epoch_ts, range_start)`

## OPT_IDLE_005: Inactive Account

- -- Flags non-system accounts with no connections or byte throughput across a configurable threshold window.
- -- Returns inactive_secs: seconds since last active epoch, or -1 if never active.
- -- Severity: info | Threshold: zero connections and zero throughput within p_threshold_secs (excludes system)
- -- DEVIATION: Joins hx.account_opts for is_system filter, hx.account_ident for name.
- **Macro**: `audit.check_opt_idle_005(epoch_ts, range_start, p_threshold_secs := 86400)`

## OPT_IDLE_006: Disconnected Users

- -- Flags non-system account users that have no active client connections at the current epoch.
- -- Severity: info | Threshold: no active connections at current epoch (excludes system)
- -- DEVIATION: Joins hx.account_opts for is_system filter, hx.account_ident for name.
- **Macro**: `audit.check_opt_idle_006(epoch_ts, range_start)`

## OPT_IDLE_007: Idle Client Connections

- -- Flags client connections that have been idle for more than 5 minutes and have
- -- sent and received zero messages.
- -- Severity: info | Threshold: idle > 5 minutes with zero lifetime messages
- **Macro**: `audit.check_opt_idle_007(epoch_ts, range_start, p_idle_minutes := 5.0)`

## OPT_PLACE_001: Cross-Cluster Stream Access

- -- Flags accounts where client connections exist in clusters that have no stream
- -- leaders, forcing all stream access through gateways.
- -- Severity: info | Threshold: clients in cluster with no local stream leaders,
- -- with at least p_min_conn_count connections making up at least p_min_conn_pct
- -- of the account's connections (a handful of stray clients is not actionable)
- -- DEVIATION: Joins hx.conn_ident + hx.conn_stats + hx.server_ident for
- -- connection cluster, hx.stream_replica_stats + hx.stream_ident +
- -- hx.server_ident for stream leader cluster, and hx.account_ident for name.
- **Macro**: `audit.check_opt_place_001(epoch_ts, range_start, p_min_conn_pct := 10.0, p_min_conn_count := 5)`

## OPT_PLACE_002: Consumer Leader Not Co-located

- -- Flags consumers whose leader server is in a different cluster than the majority
- -- of the account's client connections.
- -- Severity: info | Threshold: consumer leader in different cluster than the
- -- dominant connection cluster, requiring at least p_min_conns total account
- -- connections and the dominant cluster holding at least p_min_dominance_pct of
- -- them ("majority" off a couple of clients is noise)
- -- DEVIATION: Joins hx.consumer_replica_stats + hx.consumer_ident +
- -- hx.stream_ident for account, hx.server_ident for clusters, hx.conn_ident +
- -- hx.conn_stats for connection distribution, and hx.account_ident for name.
- **Macro**: `audit.check_opt_place_002(epoch_ts, range_start, p_min_conns := 5, p_min_dominance_pct := 60.0)`

## OPT_PLACE_003: High Gateway Traffic Ratio

- -- Flags accounts where more than p_warn_percent of the byte throughput within
- -- the selected time range is cross-cluster gateway traffic, suggesting
- -- placement optimization.
- -- Severity: info | Threshold: in-window gateway byte delta >= p_warn_percent of
- -- the in-window total byte delta, with at least p_min_total_bytes moved
- -- DEVIATION: Joins hx.account_stats directly (not via view) and
- -- hx.account_ident for account name. Byte columns are monotonic per-server
- -- counters, so deltas are computed per (account, server) between the first and
- -- last samples in the window, clamped at zero to absorb counter resets, then
- -- summed per account. The percentage is capped at 100 as a sanity guard.
- **Macro**: `audit.check_opt_place_003(epoch_ts, range_start, p_warn_percent := 30.0, p_min_total_bytes := 1048576)`

## OPT_PLACE_004: Gateway Interest Mode

- -- Flags account/remote-cluster combinations still using optimistic interest
- -- mode. Every local server in a cluster runs its own gateway to the remote
- -- cluster, so rows are aggregated per (account, remote cluster) to avoid one
- -- near-duplicate finding per local server; the lowest local server pk is the
- -- representative entity.
- -- Severity: info | Threshold: interest_mode = 'Optimistic'
- -- Extra columns: account (VARCHAR), remote_cluster (VARCHAR),
- -- no_interest_count (BIGINT), interest_only_threshold (BIGINT),
- -- total_subscriptions (BIGINT), local_server_count (BIGINT)
- **Macro**: `audit.check_opt_place_004(epoch_ts, range_start)`

## OPT_SYS_001: Streams Without Limits

- -- Flags streams with no message, byte, or age retention limits. Unbounded streams
- -- risk uncontrolled disk growth.
- -- Severity: info | Threshold: max_msgs = -1 AND max_bytes = -1 AND max_age = 0 AND max_msgs_per_subject = -1 (excludes sealed)
- -- DEVIATION: Joins hx.account_ident for account name (not on hx.streams view).
- -- Extra columns: (none)
- **Macro**: `audit.check_opt_sys_001(epoch_ts, range_start)`

## OPT_SYS_002: High Consumer Redelivery

- -- Flags consumers whose current redelivery backlog exceeds 10% of the messages
- -- delivered within the audit window. High redelivery indicates processing
- -- failures or ack timeouts.
- -- Severity: warning | Threshold: num_redelivered > 10% of in-window delivered delta
- -- DEVIATION: Joins hx.stream_ident and hx.account_ident for entity name; the
- -- window CTE reads hx.consumer_replica_stats directly so the delta does not
- -- depend on opts rows existing at every epoch in the window.
- -- Extra columns: redelivery_pct (DOUBLE), num_redelivered (BIGINT), delivered (BIGINT)
- **Macro**: `audit.check_opt_sys_002(epoch_ts, range_start, p_redelivery_percent := 10.0)`

## OPT_SYS_003: Ack Pending Buildup

- -- Flags consumers approaching their maximum ack pending limit. Reaching the limit
- -- stalls message delivery.
- -- Severity: warning | Threshold: num_ack_pending >= 80% of max_ack_pending
- -- DEVIATION: Joins hx.stream_ident and hx.account_ident for entity name.
- -- Extra columns: current_val (BIGINT), max_val (BIGINT), pct (DOUBLE), unit (VARCHAR)
- **Macro**: `audit.check_opt_sys_003(epoch_ts, range_start, p_warn_percent := 80.0)`

## OPT_SYS_004: Unbound Push Consumer

- -- Flags push consumers with a deliver subject configured but no subscriber currently bound.
- -- Severity: warning | Threshold: push consumer with no bound subscriber
- -- DEVIATION: Joins hx.stream_ident and hx.account_ident for entity name.
- -- Extra columns: deliver_subject (VARCHAR)
- **Macro**: `audit.check_opt_sys_004(epoch_ts, range_start)`

## OPT_SYS_005: Route Pending Pressure

- -- Flags route connections with more than 1 MiB of pending data.
- -- Severity: warning | Threshold: pending_size > 1 MiB
- -- Extra columns: remote_pk (BIGINT), remote_name (VARCHAR), pending_mib (DOUBLE)
- **Macro**: `audit.check_opt_sys_005(epoch_ts, range_start, p_pending_mib := 1.0)`

## OPT_SYS_006: Leaf Compression Disabled

- -- Flags leaf connections with compression disabled. Enabling compression reduces
- -- bandwidth between leaf and hub.
- -- Severity: info | Threshold: compression = 'off'
- -- Extra columns: (none)
- **Macro**: `audit.check_opt_sys_006(epoch_ts, range_start)`

## OPT_SYS_007: Raft Apply Lag

- -- Flags Raft groups where the gap between committed and applied log entries exceeds 100.
- -- Nodes that are catching up are excluded: their lag is expected and self-healing,
- -- and OPT_SYS_013 covers sustained catch-up.
- -- Severity: warning | Threshold: committed - applied > 100 AND catching_up = false
- -- Extra columns: apply_lag (BIGINT), committed (BIGINT), applied (BIGINT)
- **Macro**: `audit.check_opt_sys_007(epoch_ts, range_start, p_max_lag := 100)`

## OPT_SYS_008: Unlimited JetStream Account

- -- Flags non-system accounts with JetStream enabled but no memory or disk storage limits.
- -- Severity: info | Threshold: js_mem_storage = -1 AND js_disk_storage = -1 (excludes system)
- -- Uses a CTE to get distinct active accounts and then checks opts to avoid
- -- duplicates from the per-server account_stats rows in the hx.accounts view.
- -- Extra columns: (none)
- **Macro**: `audit.check_opt_sys_008(epoch_ts, range_start)`

## OPT_SYS_009: Leaderless Raft Group

- -- Flags raft groups with no elected leader. Leaderless groups cannot process writes.
- -- Severity: critical | Threshold: leader = '' AND ever_had_leader = true
- -- Extra columns: (none)
- **Macro**: `audit.check_opt_sys_009(epoch_ts, range_start)`

## OPT_SYS_010: Raft IPQ Backpressure

- -- Flags raft groups where any internal queue length exceeds the threshold.
- -- Severity: warning | Threshold: GREATEST(ipq_prop_len, ipq_entry_len, ipq_resp_len, ipq_apply_len) > p_max_ipq
- -- Extra columns: max_ipq_len (BIGINT), ipq_prop_len (BIGINT), ipq_entry_len (BIGINT), ipq_resp_len (BIGINT), ipq_apply_len (BIGINT), group_name (VARCHAR)
- **Macro**: `audit.check_opt_sys_010(epoch_ts, range_start, p_max_ipq := 1000)`

## OPT_SYS_011: Subscription Fanout Anomaly

- -- Flags servers where max fanout is disproportionately higher than average fanout.
- -- Severity: info | Threshold: max_fanout > multiplier * avg_fanout AND avg_fanout > 1
- -- Extra columns: max_fanout (INTEGER), avg_fanout (DOUBLE)
- **Macro**: `audit.check_opt_sys_011(epoch_ts, range_start, p_multiplier := 10)`

## OPT_SYS_012: Subscription Churn

- -- Flags servers with excessive subscription insert and remove operations since the previous epoch.
- -- Severity: info | Threshold: churn delta > max_churn
- -- Extra columns: churn (BIGINT)
- **Macro**: `audit.check_opt_sys_012(epoch_ts, range_start, p_max_churn := 10000)`

## OPT_SYS_013: Raft Sustained Catching Up

- -- Flags raft groups where a node is catching up to the leader.
- -- Severity: warning | Threshold: catching_up = true
- -- Extra columns: group_name (VARCHAR)
- **Macro**: `audit.check_opt_sys_013(epoch_ts, range_start)`

## OPT_SYS_014: Gateway Pending Pressure

- -- Flags gateway connections with more than 1 MiB of pending data.
- -- Severity: warning | Threshold: pending_size > 1 MiB
- -- Extra columns: remote_pk (BIGINT), remote_name (VARCHAR), pending_mib (DOUBLE)
- **Macro**: `audit.check_opt_sys_014(epoch_ts, range_start, p_pending_mib := 1.0)`

## OPT_SYS_015: Consumer ACK Floor Divergence

- -- Flags consumers where the gap between delivered position and ACK floor is disproportionately
- -- large relative to max_ack_pending, or exceeds absolute thresholds.
- -- Severity: dynamic (info/warning) based on relative multiplier and absolute fallback
- -- DEVIATION: Joins hx.stream_ident and hx.account_ident for entity name.
- -- Extra columns: gap (BIGINT), max_ack_pending (BIGINT), gap_ratio (DOUBLE)
- **Macro**: `audit.check_opt_sys_015(epoch_ts, range_start, p_warn_multiplier := 2.0, p_crit_multiplier := 5.0, p_warn_abs := 100000, p_crit_abs := 1000000)`

## OPT_SYS_016: Direct Gets Disabled

- -- Flags replicated streams with allow_direct disabled, forcing read operations
- -- through Raft. R1 streams are excluded: reads hit the single server either way,
- -- so the Raft-latency rationale does not apply.
- -- Severity: info | Threshold: allow_direct = false AND num_replicas > 1
- -- DEVIATION: Joins hx.account_ident for account name.
- -- Extra columns: (none)
- **Macro**: `audit.check_opt_sys_016(epoch_ts, range_start)`

## OPT_SYS_017: Leafnode Auto Compression with High Count

- -- Flags servers with many leafnode connections using s2_auto compression.
- -- Severity: info | Threshold: server has > p_min_leafs leafnodes with any using s2_auto
- -- Extra columns: leaf_count (BIGINT), auto_count (BIGINT)
- **Macro**: `audit.check_opt_sys_017(epoch_ts, range_start, p_min_leafs := 20)`

## OPT_SYS_018: High Interior Deletes on Stream

- -- Flags streams with a very high number of interior deletes, causing memory pressure.
- -- Severity: warning | Threshold: num_deleted > p_max_deleted OR delete ratio > 90%
- -- DEVIATION: Joins hx.account_ident for account name.
- -- Extra columns: num_deleted (BIGINT), msgs (BIGINT), delete_ratio (DOUBLE)
- **Macro**: `audit.check_opt_sys_018(epoch_ts, range_start, p_max_deleted := 100000000)`

## OPT_SYS_019: Large Deduplication Window

- -- Flags streams with a deduplication window exceeding the threshold and active message flow.
- -- Severity: dynamic (info/warning) based on window duration with rate gate
- -- DEVIATION: Joins hx.account_ident for account name. Computes message rate from consecutive epochs.
- -- Extra columns: dedup_window_minutes (DOUBLE), msg_rate_per_sec (DOUBLE)
- **Macro**: `audit.check_opt_sys_019(epoch_ts, range_start, p_warn_window_minutes := 60.0, p_crit_window_minutes := 360.0, p_min_rate_per_sec := 10.0)`

## OPT_SYS_020: KV Buckets Without max_age

- -- Flags KV buckets with no max_age that have accumulated many interior deletes.
- -- Severity: info | Threshold: is_kv AND max_age = 0 AND num_deleted > p_min_deleted
- -- DEVIATION: Joins hx.account_ident for account name.
- -- Extra columns: num_deleted (BIGINT)
- **Macro**: `audit.check_opt_sys_020(epoch_ts, range_start, p_min_deleted := 100000)`

## OPT_SYS_021: R1 Streams in Multi-Node Clusters

- -- Flags R1 (single-replica) streams in multi-node clusters with no redundancy.
- -- Severity: info | Threshold: num_replicas = 1 in cluster with > 1 JetStream node
- -- DEVIATION: Joins server_ident for cluster, counts nodes per cluster.
- -- Cluster names are trimmed (empty -> NULL) so whitespace variants collapse
- -- into one cluster (same idiom as CLUSTER_005).
- -- Extra columns: cluster_nodes (BIGINT)
- **Macro**: `audit.check_opt_sys_021(epoch_ts, range_start)`

## OPT_SYS_022: Subscription Count Growth

- -- Flags servers where subscriptions grow monotonically without corresponding connection growth.
- -- Requires 10+ epochs in the lookback window, at least p_min_mono_frac of which
- -- are non-decreasing, so a single transient dip does not disqualify a real leak.
- -- Severity: info | Threshold: >= p_min_mono_frac of epochs monotonic over p_min_epochs epochs, total > p_min_growth_pct%
- -- Extra columns: start_subs (BIGINT), end_subs (BIGINT), growth_pct (DOUBLE), conn_delta_pct (DOUBLE), monotonic_epochs (BIGINT)
- **Macro**: `audit.check_opt_sys_022(epoch_ts, range_start, p_min_epochs := 10, p_min_growth_pct := 20.0, p_min_mono_frac := 0.8)`

## OPT_SYS_023: Raft WAL Size Excessive

- -- Flags raft groups with an excessively large write-ahead log.
- -- Triggers on absolute size OR relative to js_max_store.
- -- Severity: dynamic (warning/critical) based on absolute and relative thresholds
- -- Extra columns: wal_gib (DOUBLE), group_name (VARCHAR), pct_of_max_store (DOUBLE)
- **Macro**: `audit.check_opt_sys_023(epoch_ts, range_start, p_warn_bytes_gib := 10.0, p_crit_bytes_gib := 50.0, p_warn_pct := 50.0, p_crit_pct := 80.0)`

## OPT_SYS_024: WorkQueue Discard New with Aggressive Consumer Settings

- -- Flags WorkQueue streams using discard_policy=new where consumers have aggressive
- -- ack_wait or max_deliver settings, risking message loss.
- -- Severity: warning | Threshold: workqueue + discard new + (ack_wait < 30s OR max_deliver < 10)
- -- DEVIATION: Joins hx.stream_ident, hx.account_ident for entity name.
- -- Extra columns: consumer_name (VARCHAR), ack_wait_secs (DOUBLE), max_deliver (INTEGER)
- **Macro**: `audit.check_opt_sys_024(epoch_ts, range_start, p_min_ack_wait_secs := 30, p_min_max_deliver := 10)`

## OPT_SYS_025: Sustained Consumer Growth on Stream

- -- Flags streams where consumer count has been growing over the lookback window.
- -- Requires at least p_min_mono_frac of consecutive epochs to show non-decreasing
- -- consumer count, so a single transient dip does not disqualify a real leak, and
- -- total growth over the window of at least p_min_total_growth (a fixed window
- -- total, independent of scrape density).
- -- Severity: warning | Threshold: >= p_min_mono_frac monotonic epochs over >= min_growth_epochs, total growth >= p_min_total_growth
- -- Extra columns: start_consumers (BIGINT), end_consumers (BIGINT), growth_epochs (BIGINT)
- **Macro**: `audit.check_opt_sys_025(epoch_ts, range_start, p_min_growth_epochs := 5, p_min_total_growth := 500, p_min_mono_frac := 0.8)`

## OPT_SYS_026: Raft Group Peer Count Mismatch

- -- Flags Raft groups where the observed peer count exceeds the expected num_replicas
- -- from the corresponding stream or consumer configuration.
- -- Severity: warning | Threshold: raft size > expected num_replicas
- -- Two NATS-specific resolutions:
- --   1. consumer num_replicas=0 means "inherit from parent stream" (NATS API
- --      convention) — resolve via the parent stream's latest stream_opts.
- --   2. raft_group names can be reused across stream_pks (e.g., a stream was
- --      deleted and a new one took the same group name). For each raft_group
- --      we keep only the most recently configured row so stale config from a
- --      prior owner doesn't drive the comparison.
- -- Every hosting server reports the same raft group, so findings are
- -- deduplicated to one row per (group, epoch), preferring the leader's row.
- -- Extra columns: group_name (VARCHAR), observed_size (BIGINT), expected_replicas (BIGINT)
- **Macro**: `audit.check_opt_sys_026(epoch_ts, range_start)`

## SERVER_001: Connection Readiness Failure

- -- Flags servers with CONNECTION-type healthz errors or unhealthy with no error details.
- -- Severity: critical | Threshold: healthz status != "ok" with CONNECTION error type
- -- Extra columns: error_text (VARCHAR)
- **Macro**: `audit.check_server_001(epoch_start, epoch_end)`

## SERVER_002: Server Version Mismatch

- -- Identifies servers running a different software version than their cluster's majority.
- -- The majority is computed per (epoch, cluster) so an intentionally-older cluster in a
- -- supercluster is not flagged against another cluster's version. Cluster names are normalized
- -- (NULLIF(trim(...))) per the CLUSTER_005/OPT_SYS_021 idiom; unclustered servers (empty/NULL
- -- cluster) share a single pseudo-cluster bucket, so a lone unclustered server never fires.
- -- Severity: warning | Threshold: version differs from the cluster majority
- -- Extra columns: old_value (VARCHAR), new_value (VARCHAR)
- **Macro**: `audit.check_server_002(epoch_start, epoch_end)`

## SERVER_003: High CPU Usage

- -- Flags servers where per-core CPU usage meets or exceeds 90%.
- -- The hx.servers view normalizes cpu to per-core (cpu / cores).
- -- Severity: warning | Threshold: cpu >= 90%
- -- Extra columns: cpu_percent (DOUBLE)
- **Macro**: `audit.check_server_003(epoch_start, epoch_end, p_cpu_percent := 90.0)`

## SERVER_004: Slow Consumers

- -- Flags servers with new slow consumer events since the previous epoch.
- -- Severity: critical | Threshold: slow_consumers delta > 0
- -- Extra columns: slow_consumers (BIGINT)
- **Macro**: `audit.check_server_004(epoch_start, epoch_end)`

## SERVER_005: JetStream Memory Pressure

- -- Flags servers where JetStream memory usage is at or above 90% of reserved.
- -- Severity: warning | Threshold: memory >= 90% of reserved
- -- Extra columns: pct (DOUBLE)
- **Macro**: `audit.check_server_005(epoch_start, epoch_end, p_warn_percent := 90.0)`

## SERVER_006: JetStream Domain Whitespace

- -- Flags servers whose JetStream domain name contains whitespace characters.
- -- Severity: warning | Threshold: whitespace in JS domain
- -- Extra columns: js_domain (VARCHAR)
- **Macro**: `audit.check_server_006(epoch_start, epoch_end)`

## SERVER_007: Authentication Not Required

- -- Flags servers that do not require client authentication.
- -- Severity: critical | Threshold: auth_required = false
- -- Extra columns: (none)
- **Macro**: `audit.check_server_007(epoch_start, epoch_end)`

## SERVER_008: Unexpected Server Restart

- -- Detects servers that restarted without an accompanying version upgrade.
- -- hx.server_ident is written once per pk and a restart mints a new server ID
- -- (and therefore a new pk), so identity history is keyed by server NAME: a
- -- restart shows up as the name carrying a different start_time (a different
- -- boot generation / pk) than at the previous epoch. Excludes restarts where
- -- the server version changed between consecutive epochs (planned upgrade).
- -- A same-version restart cannot be distinguished from a crash purely from
- -- /varz, so this check fires on any unplanned same-version restart.
- -- Severity: critical | Threshold: start_time changed for the name, version unchanged
- -- Extra columns: (none)
- **Macro**: `audit.check_server_008(epoch_start, epoch_end)`

## SERVER_009: Server Crash Loop

- -- Flags servers that show multiple distinct start times within the rolling
- -- 1-hour window, indicating repeated restarts (crash loop). hx.server_ident is
- -- written once per pk and a restart mints a new pk, so boot generations are
- -- counted per server NAME: distinct start_time values among the pks the name
- -- reported stats under within the window. restart_count is the number of boot
- -- generations observed (N generations = N-1 actual restarts).
- -- Severity: critical | Threshold: distinct start_time count > 2 (configurable)
- -- Extra columns: restart_count (BIGINT)
- **Macro**: `audit.check_server_009(epoch_start, epoch_end, p_max_restarts := 2)`

## SERVER_010: High Route RTT

- -- Flags route connections with RTT exceeding 50ms.
- -- Severity: warning | Threshold: rtt > 50ms (50,000,000 nanoseconds)
- -- Extra columns: remote_pk (BIGINT), remote_name (VARCHAR), rtt_ms (DOUBLE)
- **Macro**: `audit.check_server_010(epoch_start, epoch_end, p_rtt_ms := 50.0)`

## SERVER_011: Connection Count High

- -- Flags servers where active connections are at or above 80% of max_connections.
- -- Severity: warning | Threshold: connections >= 80% of max_connections
- -- Extra columns: current_val (BIGINT), max_val (BIGINT), pct (DOUBLE), unit (VARCHAR)
- **Macro**: `audit.check_server_011(epoch_start, epoch_end, p_warn_percent := 80.0)`

## SERVER_012: Stale Connections

- -- Flags servers with new stale connection events since the previous epoch.
- -- Severity: warning | Threshold: stale_connections delta > 0
- -- Extra columns: stale_connections (BIGINT)
- **Macro**: `audit.check_server_012(epoch_start, epoch_end)`

## SERVER_013: Stalled Clients

- -- Flags servers with new stalled client events since the previous epoch.
- -- Severity: warning | Threshold: stalled_clients delta > 0
- -- Extra columns: stalled_clients (BIGINT)
- **Macro**: `audit.check_server_013(epoch_start, epoch_end)`

## SERVER_014: JetStream Subsystem Unhealthy

- -- Flags servers with JETSTREAM-type healthz errors.
- -- Severity: critical | Threshold: healthz JETSTREAM error type
- -- Extra columns: error_text (VARCHAR)
- **Macro**: `audit.check_server_014(epoch_start, epoch_end)`

## SERVER_015: Stream Recovery Failure

- -- Flags servers with STREAM or CONSUMER-type healthz errors.
- -- Severity: critical | Threshold: healthz STREAM or CONSUMER error type
- -- Extra columns: error_text (VARCHAR)
- **Macro**: `audit.check_server_015(epoch_start, epoch_end)`

## SERVER_016: Account Resolution Failure

- -- Flags servers with ACCOUNT-type healthz errors.
- -- Severity: warning | Threshold: healthz ACCOUNT error type
- -- Extra columns: error_text (VARCHAR)
- **Macro**: `audit.check_server_016(epoch_start, epoch_end)`

## SERVER_017: JetStream Storage Pressure

- -- Flags servers where JetStream storage usage is at or above 90% of reserved.
- -- Severity: warning | Threshold: storage >= 90% of reserved
- -- Extra columns: pct (DOUBLE)
- **Macro**: `audit.check_server_017(epoch_start, epoch_end, p_warn_percent := 90.0)`

## SERVER_018: High Gateway RTT

- -- Flags gateway connections with RTT exceeding 50ms.
- -- Severity: warning | Threshold: rtt > 50ms (50,000,000 nanoseconds)
- -- Extra columns: remote_pk (BIGINT), remote_name (VARCHAR), rtt_ms (DOUBLE)
- **Macro**: `audit.check_server_018(epoch_start, epoch_end, p_rtt_ms := 50.0)`

## SERVER_019: JetStream Storage vs Configured Limit

- -- Flags servers where JetStream storage usage approaches the configured
- -- max_store limit. This differs from SERVER_017, which uses reserved storage
- -- as its denominator.
- -- Severity: dynamic (warning at >= p_warn_percent, critical at >= p_crit_percent)
- -- Threshold: js_storage >= 90% of js_max_store (default)
- -- Extra columns: pct (DOUBLE)
- **Macro**: `audit.check_server_019(epoch_start, epoch_end, p_warn_percent := 90.0, p_crit_percent := 95.0)`

## SERVICE_001: Service Version Mismatch

- -- Flags services where instances report different client versions or languages.
- -- Severity: warning | Detects heterogeneous deployments of the same service.
- -- Extra columns: versions (VARCHAR)
- **Macro**: `audit.check_service_001(epoch_start, epoch_end)`

## SERVICE_002: Service Down

- -- Flags services that had instances in the previous epoch but zero in the current epoch.
- -- Severity: critical | Detects service disappearances.
- -- Extra columns: (none)
- **Macro**: `audit.check_service_002(epoch_start, epoch_end)`

## USER_001: Bearer Token User

- -- Flags bearer token users with active connections.
- -- Severity: warning | Threshold: bearer = true with active connections
- **Macro**: `audit.check_user_001(epoch_start, epoch_end)`

## USER_002: Excessive User Connections

- -- Flags users with more than 100 active connections.
- -- Severity: warning | Threshold: connections > 100
- **Macro**: `audit.check_user_002(epoch_start, epoch_end, p_max_connections := 100)`
