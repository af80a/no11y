CREATE OR REPLACE MACRO audit.check_opt_idle_005(epoch_ts, range_start, p_threshold_secs := 86400) AS TABLE
  -- range_start is accepted for interface compatibility with buildCheckResultsInsert but not used.
  -- The macro computes its own effective range from p_threshold_secs.
  -- Byte counters are per-server monotonic counters; computing MAX-MIN across
  -- the (account x server) cross product would report inter-server counter
  -- spread as throughput. Aggregate deltas per server first, then sum.
  WITH server_agg AS (
    SELECT
      s.account_pk,
      s.server_pk,
      SUM(s.conns) AS total_conns,
      arg_max(s.bytes_sent, s.epoch) - arg_min(s.bytes_sent, s.epoch) AS bytes_sent_delta,
      arg_max(s.bytes_recv, s.epoch) - arg_min(s.bytes_recv, s.epoch) AS bytes_recv_delta
    FROM hx.account_stats s
    WHERE s.epoch BETWEEN (epoch_ts - INTERVAL (p_threshold_secs) SECOND) AND epoch_ts
    GROUP BY s.account_pk, s.server_pk
  ),
  account_agg AS (
    SELECT
      account_pk,
      SUM(total_conns) AS total_conns,
      SUM(bytes_sent_delta) AS bytes_sent_delta,
      SUM(bytes_recv_delta) AS bytes_recv_delta
    FROM server_agg
    GROUP BY account_pk
  ),
  latest_opts AS (
    SELECT DISTINCT ON (account_pk) account_pk, is_system
    FROM hx.account_opts
    WHERE epoch <= epoch_ts
    ORDER BY account_pk, epoch DESC
  ),
  last_active AS (
    SELECT
      s.account_pk,
      MAX(s.epoch) AS last_active_epoch
    FROM hx.account_stats s
    WHERE s.conns > 0
      AND s.epoch <= epoch_ts
    GROUP BY s.account_pk
  )
  SELECT
    'OPT_IDLE_005' AS code,
    ai.name AS entity,
    ai.pk AS entity_pk,
    COALESCE(EXTRACT(EPOCH FROM (epoch_ts - la.last_active_epoch)), -1) AS inactive_secs,
    epoch_ts AS epoch
  FROM account_agg a
  INNER JOIN hx.account_ident ai ON ai.pk = a.account_pk
  INNER JOIN latest_opts o ON o.account_pk = a.account_pk
  LEFT JOIN last_active la ON la.account_pk = a.account_pk
  WHERE o.is_system = false
    AND a.total_conns = 0
    AND a.bytes_sent_delta = 0
    AND a.bytes_recv_delta = 0;
