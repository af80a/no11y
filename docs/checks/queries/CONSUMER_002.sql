CREATE OR REPLACE MACRO audit.check_consumer_002(epoch_start, epoch_end, p_max_lag := 1000) AS TABLE
  SELECT
    'CONSUMER_002' AS code,
    ai.name || ' / ' || si.name || ' / ' || ci.name AS entity,
    ci.pk AS entity_pk,
    srv.pk AS remote_pk,
    srv.name AS remote_name,
    CAST(r.lag AS DOUBLE) AS lag,
    r.epoch
  FROM hx.consumer_replica_stats r
  INNER JOIN hx.consumer_ident ci ON ci.pk = r.consumer_pk
  INNER JOIN hx.stream_ident si ON si.pk = ci.stream_pk
  INNER JOIN hx.account_ident ai ON ai.pk = si.account_pk
  INNER JOIN hx.server_ident srv ON r.peer_server_pk = srv.pk
  WHERE r.epoch BETWEEN epoch_start AND epoch_end
    AND r.peer_server_pk != 0 -- Exclude self-reported rows (always have lag=0).
    AND r.is_leader = false
    AND r.is_offline = false
    AND r.lag > p_max_lag;
