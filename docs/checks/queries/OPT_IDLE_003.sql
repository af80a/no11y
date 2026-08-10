CREATE OR REPLACE MACRO audit.check_opt_idle_003(epoch_ts, range_start) AS TABLE
  -- Window aggregation rather than exact-epoch endpoints: range_start is a
  -- computed boundary that rarely aligns with a scraped epoch.
  WITH consumer_agg AS (
    SELECT
      r.consumer_pk,
      arg_min(r.delivered_stream_seq, r.epoch) AS first_delivered,
      arg_max(r.delivered_stream_seq, r.epoch) AS last_delivered,
      arg_max(r.num_pending, r.epoch) AS num_pending
    FROM hx.consumer_replica_stats r
    WHERE r.epoch BETWEEN range_start AND epoch_ts
      AND r.is_leader = true
    GROUP BY r.consumer_pk
    -- Require a sample at the current epoch (consumer still exists) and at
    -- least two samples (a just-created consumer has no progress to measure).
    -- MAX() wraps the constant macro param so the binder accepts it in HAVING.
    HAVING MAX(r.epoch) = MAX(epoch_ts)
      AND COUNT(*) >= 2
  )
  SELECT
    'OPT_IDLE_003' AS code,
    ai.name || ' / ' || si.name || ' / ' || ci.name AS entity,
    ci.pk AS entity_pk,
    a.last_delivered AS delivered_seq,
    a.num_pending AS num_pending,
    epoch_ts AS epoch
  FROM consumer_agg a
  INNER JOIN hx.consumer_ident ci ON ci.pk = a.consumer_pk
  INNER JOIN hx.stream_ident si ON si.pk = ci.stream_pk
  INNER JOIN hx.account_ident ai ON ai.pk = si.account_pk
  WHERE a.first_delivered = a.last_delivered;
