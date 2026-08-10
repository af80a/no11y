CREATE OR REPLACE MACRO audit.check_opt_sys_025(epoch_ts, range_start, p_min_growth_epochs := 5, p_min_total_growth := 500, p_min_mono_frac := 0.8) AS TABLE
  WITH leader_rows AS (
    -- One row per (stream, epoch): multiple replicas can briefly report
    -- leadership in the same scrape; keep a single deterministic series.
    SELECT r.stream_pk, r.epoch, r.num_consumers
    FROM hx.stream_replica_stats r
    WHERE r.epoch BETWEEN range_start AND epoch_ts
      AND r.is_leader = true
    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.stream_pk, r.epoch ORDER BY r.server_pk) = 1
  ),
  leader_stats AS (
    SELECT
      stream_pk,
      epoch,
      num_consumers,
      LAG(num_consumers) OVER (PARTITION BY stream_pk ORDER BY epoch) AS prev_consumers
    FROM leader_rows
  ),
  monotonic AS (
    SELECT
      stream_pk,
      COUNT(*) AS epoch_count,
      SUM(CASE WHEN prev_consumers IS NULL OR num_consumers >= prev_consumers THEN 1 ELSE 0 END) AS mono_count,
      FIRST(num_consumers ORDER BY epoch) AS start_consumers,
      LAST(num_consumers ORDER BY epoch) AS end_consumers
    FROM leader_stats
    GROUP BY stream_pk
  )
  SELECT
    'OPT_SYS_025' AS code,
    ai.name || ' / ' || si.name AS entity,
    m.stream_pk AS entity_pk,
    CASE WHEN so.is_kv THEN 'kvstore' WHEN so.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    m.start_consumers::BIGINT AS start_consumers,
    m.end_consumers::BIGINT AS end_consumers,
    m.epoch_count::BIGINT AS growth_epochs,
    epoch_ts AS epoch
  FROM monotonic m
  INNER JOIN hx.stream_ident si ON m.stream_pk = si.pk
  INNER JOIN hx.stream_opts so ON m.stream_pk = so.stream_pk
    AND so.epoch = (SELECT MAX(epoch) FROM hx.stream_opts
                    WHERE stream_pk = so.stream_pk AND epoch <= epoch_ts)
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
  WHERE m.epoch_count >= p_min_growth_epochs
    AND m.mono_count >= ceil(p_min_mono_frac * m.epoch_count)
    AND m.end_consumers > m.start_consumers
    AND (m.end_consumers - m.start_consumers) >= p_min_total_growth;
