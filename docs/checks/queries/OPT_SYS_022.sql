CREATE OR REPLACE MACRO audit.check_opt_sys_022(epoch_ts, range_start, p_min_epochs := 10, p_min_growth_pct := 20.0, p_min_mono_frac := 0.8) AS TABLE
  WITH ordered AS (
    SELECT
      s.server_pk,
      s.epoch,
      s.subscriptions,
      s.connections,
      LAG(s.subscriptions) OVER (PARTITION BY s.server_pk ORDER BY s.epoch) AS prev_subs
    FROM hx.server_stats s
    WHERE s.epoch BETWEEN range_start AND epoch_ts
  ),
  -- Count epochs in the lookback window that are monotonically non-decreasing
  monotonic AS (
    SELECT
      server_pk,
      COUNT(*) AS total_epochs,
      -- Count how many epochs have subs >= prev_subs (monotonic non-decreasing)
      SUM(CASE WHEN prev_subs IS NULL OR subscriptions >= prev_subs THEN 1 ELSE 0 END) AS mono_count,
      FIRST(subscriptions ORDER BY epoch) AS start_subs,
      LAST(subscriptions ORDER BY epoch) AS end_subs,
      FIRST(connections ORDER BY epoch) AS start_conns,
      LAST(connections ORDER BY epoch) AS end_conns
    FROM ordered
    GROUP BY server_pk
  )
  SELECT
    'OPT_SYS_022' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    m.server_pk AS entity_pk,
    m.start_subs::BIGINT AS start_subs,
    m.end_subs::BIGINT AS end_subs,
    round((m.end_subs - m.start_subs) * 100.0 / m.start_subs, 1) AS growth_pct,
    CASE WHEN m.start_conns > 0
      THEN round((m.end_conns - m.start_conns) * 100.0 / m.start_conns, 1)
      ELSE 0 END AS conn_delta_pct,
    m.mono_count::BIGINT AS monotonic_epochs,
    epoch_ts AS epoch
  FROM monotonic m
  INNER JOIN hx.server_ident srv ON m.server_pk = srv.pk
  WHERE m.total_epochs >= p_min_epochs
    AND m.start_subs > 0
    AND m.end_subs > m.start_subs
    -- Most epochs must be monotonic (allowing first epoch which has no prev);
    -- tolerate transient dips below the configured fraction
    AND m.mono_count >= ceil(p_min_mono_frac * m.total_epochs)
    AND (m.end_subs - m.start_subs) * 100.0 / m.start_subs > p_min_growth_pct
    AND (m.start_conns = 0 OR abs(m.end_conns - m.start_conns) * 100.0 / m.start_conns < 5);
