CREATE OR REPLACE MACRO audit.check_meta_004(epoch_start, epoch_end, p_warn_seconds := 5.0, p_crit_seconds := 30.0) AS TABLE
  SELECT
    'META_004' AS code,
    CASE WHEN m.snapshot_duration_ns >= CAST(p_crit_seconds * 1000000000::BIGINT AS BIGINT) THEN 2 ELSE 1 END AS severity,
    reporter.name AS entity,
    reporter.pk AS entity_pk,
    round(m.snapshot_duration_ns / 1000000000.0, 1) AS snapshot_seconds,
    m.epoch
  FROM hx.meta_cluster_stats m
  INNER JOIN hx.server_ident reporter ON m.server_pk = reporter.pk
  WHERE m.epoch BETWEEN epoch_start AND epoch_end
    AND m.is_leader = true
    AND m.snapshot_duration_ns > CAST(p_warn_seconds * 1000000000::BIGINT AS BIGINT);
