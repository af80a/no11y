CREATE OR REPLACE MACRO audit.check_jetstream_003(epoch_start, epoch_end, p_warn_percent := 90.0) AS TABLE
  SELECT
    'JETSTREAM_003' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    s.msgs AS current_val,
    s.max_msgs AS max_val,
    round(s.msgs * 100.0 / s.max_msgs, 1) AS pct,
    s.epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.is_leader = true
    AND s.max_msgs > 0
    AND s.max_msgs != -1
    AND s.msgs * 100.0 / s.max_msgs >= p_warn_percent;
