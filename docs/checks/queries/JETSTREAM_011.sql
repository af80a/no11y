CREATE OR REPLACE MACRO audit.check_jetstream_011(epoch_start, epoch_end, p_warn_percent := 90.0) AS TABLE
  SELECT
    'JETSTREAM_011' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    s.num_consumers AS current_val,
    s.max_consumers AS max_val,
    round(s.num_consumers * 100.0 / s.max_consumers, 1) AS pct,
    s.epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.is_leader = true
    AND s.max_consumers > 0
    AND s.max_consumers != -1
    AND s.num_consumers * 100.0 / s.max_consumers >= p_warn_percent;
