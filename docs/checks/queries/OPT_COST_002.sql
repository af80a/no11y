CREATE OR REPLACE MACRO audit.check_opt_cost_002(epoch_ts, range_start, p_max_memory_mib := 100.0) AS TABLE
  SELECT
    'OPT_COST_002' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    round(s.bytes / 1048576.0, 1) AS bytes_mib,
    epoch_ts AS epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch = epoch_ts
    AND s.is_leader = true
    AND s.storage_type = 'memory'
    AND s.bytes > p_max_memory_mib * 1048576;
