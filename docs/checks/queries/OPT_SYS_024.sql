CREATE OR REPLACE MACRO audit.check_opt_sys_024(epoch_ts, range_start, p_min_ack_wait_secs := 30, p_min_max_deliver := 10) AS TABLE
  SELECT
    'OPT_SYS_024' AS code,
    ai.name || ' / ' || si.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    c.name AS consumer_name,
    round(c.ack_wait / 1000000000.0, 1) AS ack_wait_secs,
    c.max_deliver,
    epoch_ts AS epoch
  FROM hx.streams s
  INNER JOIN hx.stream_ident si ON s.pk = si.pk
  INNER JOIN hx.account_ident ai ON si.account_pk = ai.pk
  INNER JOIN hx.consumers c ON c.stream_pk = s.pk AND c.epoch = s.epoch
    AND c.is_leader = true
  WHERE s.epoch = epoch_ts
    AND s.is_leader = true
    AND s.retention_policy = 'workqueue'
    AND s.discard_policy = 'new'
    -- max_deliver < 1 means unlimited redeliveries: never flag it as low,
    -- but still flag a short ack_wait alongside it
    AND ((c.max_deliver >= 1 AND c.max_deliver < p_min_max_deliver)
      OR c.ack_wait < p_min_ack_wait_secs * 1000000000::BIGINT);
