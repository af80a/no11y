CREATE OR REPLACE MACRO audit.check_opt_idle_004(epoch_ts, range_start) AS TABLE
  WITH drained_consumers AS (
    SELECT
      r.consumer_pk
    FROM hx.consumer_replica_stats r
    WHERE r.epoch BETWEEN range_start AND epoch_ts
      AND r.is_leader = true
    GROUP BY r.consumer_pk
    -- Require a sample at the current epoch so consumers deleted mid-window
    -- don't report, and evaluate drained-ness on the current row to match the
    -- present-tense description.
    -- MAX() wraps the constant macro param so the binder accepts it in HAVING.
    HAVING MAX(r.epoch) = MAX(epoch_ts)
      AND arg_max(r.num_pending, r.epoch) = 0
      AND arg_max(r.num_ack_pending, r.epoch) = 0
  ),
  -- Window aggregation rather than exact-epoch endpoints: range_start is a
  -- computed boundary that rarely aligns with a scraped epoch.
  inactive_streams AS (
    SELECT r.stream_pk AS pk
    FROM hx.stream_replica_stats r
    WHERE r.epoch BETWEEN range_start AND epoch_ts
      AND r.is_leader = true
    GROUP BY r.stream_pk
    HAVING COUNT(*) >= 2
      AND arg_min(r.last_seq, r.epoch) = arg_max(r.last_seq, r.epoch)
  )
  SELECT
    'OPT_IDLE_004' AS code,
    ai.name || ' / ' || si.name || ' / ' || ci.name AS entity,
    ci.pk AS entity_pk,
    epoch_ts AS epoch
  FROM drained_consumers dc
  INNER JOIN hx.consumer_ident ci ON ci.pk = dc.consumer_pk
  INNER JOIN inactive_streams ist ON ist.pk = ci.stream_pk
  INNER JOIN hx.stream_ident si ON si.pk = ci.stream_pk
  INNER JOIN hx.account_ident ai ON ai.pk = si.account_pk;
