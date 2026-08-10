CREATE OR REPLACE MACRO audit.check_opt_sys_020(epoch_ts, range_start, p_min_deleted := 100000) AS TABLE
  SELECT
    'OPT_SYS_020' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    'kvstore' AS entity_type,
    s.num_deleted::BIGINT AS num_deleted,
    epoch_ts AS epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch = epoch_ts
    AND s.is_leader = true
    AND s.is_kv = true
    AND s.max_age = 0
    AND s.num_deleted > p_min_deleted;
