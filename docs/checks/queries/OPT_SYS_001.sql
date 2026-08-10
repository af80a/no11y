CREATE OR REPLACE MACRO audit.check_opt_sys_001(epoch_ts, range_start) AS TABLE
  SELECT
    'OPT_SYS_001' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    epoch_ts AS epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch = epoch_ts
    AND s.is_leader = true
    AND s.sealed = false
    AND s.max_msgs = -1
    AND s.max_bytes = -1
    AND s.max_age = 0
    AND s.max_msgs_per_subject = -1;
