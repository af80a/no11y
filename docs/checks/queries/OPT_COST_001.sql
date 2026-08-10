CREATE OR REPLACE MACRO audit.check_opt_cost_001(epoch_ts, range_start) AS TABLE
  WITH leader_now AS (
    SELECT s.pk, s.name, s.account_pk, s.num_replicas, s.last_seq, s.is_kv, s.is_object
    FROM hx.streams s
    WHERE s.epoch = epoch_ts
      AND s.is_leader = true
      AND s.num_replicas >= 3
      AND s.sealed = false
  ),
  leader_before AS (
    -- Baseline is the earliest scrape epoch inside the range: production
    -- epochs are raw wall-clock timestamps, so range_start rarely matches a
    -- scrape epoch exactly. The < epoch_ts bound prevents a degenerate
    -- self-comparison when the current epoch is the only one in range.
    SELECT r.stream_pk AS pk, r.last_seq
    FROM hx.stream_replica_stats r
    WHERE r.epoch = (
        SELECT MIN(epoch) FROM hx.stream_replica_stats
        WHERE epoch >= range_start AND epoch < epoch_ts
      )
      AND r.is_leader = true
  )
  SELECT
    'OPT_COST_001' AS code,
    ai.name || ' / ' || n.name AS entity,
    n.pk AS entity_pk,
    CASE WHEN n.is_kv THEN 'kvstore' WHEN n.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    n.num_replicas AS num_replicas,
    n.last_seq AS last_seq,
    epoch_ts AS epoch
  FROM leader_now n
  INNER JOIN leader_before b ON n.pk = b.pk
  INNER JOIN hx.account_ident ai ON n.account_pk = ai.pk
  WHERE n.last_seq = b.last_seq;
