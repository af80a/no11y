CREATE OR REPLACE MACRO audit.check_opt_sys_018(epoch_ts, range_start, p_max_deleted := 100000000) AS TABLE
  SELECT
    'OPT_SYS_018' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    s.num_deleted::BIGINT AS num_deleted,
    s.msgs::BIGINT AS msgs,
    CASE WHEN (s.msgs + s.num_deleted) > 0
      THEN round(s.num_deleted * 100.0 / (s.msgs + s.num_deleted), 1)
      ELSE 0 END AS delete_ratio,
    epoch_ts AS epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch = epoch_ts
    AND s.is_leader = true
    AND (s.num_deleted > p_max_deleted
      OR (s.num_deleted > 0 AND (s.msgs + s.num_deleted) > 0
          AND s.num_deleted * 100.0 / (s.msgs + s.num_deleted) > 90));
