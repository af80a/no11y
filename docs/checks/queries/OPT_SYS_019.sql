CREATE OR REPLACE MACRO audit.check_opt_sys_019(epoch_ts, range_start, p_warn_window_minutes := 60.0, p_crit_window_minutes := 360.0, p_min_rate_per_sec := 10.0) AS TABLE
  WITH prev_epoch AS (
    SELECT s.pk AS stream_pk, s.msgs AS prev_msgs, s.epoch AS prev_epoch
    FROM hx.streams s
    WHERE s.epoch = (
      SELECT MAX(s2.epoch)
      FROM hx.stream_replica_stats s2
      WHERE s2.stream_pk = s.pk AND s2.epoch < epoch_ts AND s2.epoch >= range_start
        AND s2.is_leader = true
    )
      AND s.is_leader = true
  )
  SELECT
    'OPT_SYS_019' AS code,
    CASE
      WHEN s.deduplication_window > p_crit_window_minutes * 60000000000 THEN 1
      ELSE 0
    END AS severity,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    round(s.deduplication_window / 60000000000.0, 1) AS dedup_window_minutes,
    CASE WHEN pe.prev_epoch IS NOT NULL AND epoch_ts > pe.prev_epoch
      THEN round((s.msgs - pe.prev_msgs) * 1.0 / extract(epoch FROM (epoch_ts - pe.prev_epoch)), 1)
      ELSE NULL END AS msg_rate_per_sec,
    epoch_ts AS epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  LEFT JOIN prev_epoch pe ON s.pk = pe.stream_pk
  WHERE s.epoch = epoch_ts
    AND s.is_leader = true
    AND s.deduplication_window > p_warn_window_minutes * 60000000000
    AND s.msgs > 0
    AND (pe.prev_epoch IS NOT NULL
      AND epoch_ts > pe.prev_epoch
      AND (s.msgs - pe.prev_msgs) * 1.0 / extract(epoch FROM (epoch_ts - pe.prev_epoch)) >= p_min_rate_per_sec);
