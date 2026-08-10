CREATE OR REPLACE MACRO audit.check_jetstream_015(epoch_start, epoch_end, p_stale_minutes := 5.0) AS TABLE
  WITH source_activity AS (
    SELECT
      s.name,
      s.account_pk,
      s.epoch,
      s.msgs
    FROM hx.streams s
    WHERE s.epoch BETWEEN epoch_start AND epoch_end
      AND s.is_leader = true
  ),
  prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.stream_replica_stats WHERE epoch < epoch_start
  ),
  source_prev AS (
    SELECT
      s.name,
      s.account_pk,
      s.msgs
    FROM hx.streams s
    CROSS JOIN prev_epoch pe
    WHERE s.epoch = pe.epoch
      AND s.is_leader = true
  )
  SELECT
    'JETSTREAM_015' AS code,
    ai.name || ' / ' || m.name AS entity,
    m.pk AS entity_pk,
    CASE WHEN m.is_kv THEN 'kvstore' WHEN m.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    round(m.mirror_active / 60000000000.0, 1) AS mirror_active_minutes,
    m.mirror_name,
    m.epoch
  FROM hx.streams m
  INNER JOIN hx.account_ident ai ON m.account_pk = ai.pk
  INNER JOIN source_activity sa ON sa.name = m.mirror_name AND sa.account_pk = m.account_pk AND sa.epoch = m.epoch
  INNER JOIN source_prev sp ON sp.name = m.mirror_name AND sp.account_pk = m.account_pk
  WHERE m.epoch BETWEEN epoch_start AND epoch_end
    AND m.is_leader = true
    AND m.is_mirror = true
    AND m.mirror_lag = 0
    AND m.mirror_active > p_stale_minutes * 60000000000
    AND sa.msgs > sp.msgs;
