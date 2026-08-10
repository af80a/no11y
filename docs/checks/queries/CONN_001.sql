CREATE OR REPLACE MACRO audit.check_conn_001(epoch_start, epoch_end, p_rtt_ms := 100.0) AS TABLE
  SELECT
    'CONN_001' AS code,
    ai.name || ' / ' || srv.name || ' / ' || CASE WHEN ci.name != '' THEN ci.name ELSE 'cid:' || ci.id END AS entity,
    ci.pk AS entity_pk,
    ci.server_pk AS server_pk,
    srv.name AS server_name,
    round(cs.rtt / 1000000.0, 1) AS rtt_ms,
    cs.epoch
  FROM hx.conn_stats cs
  INNER JOIN hx.conn_ident ci ON ci.pk = cs.conn_pk
  INNER JOIN hx.server_ident srv ON ci.server_pk = srv.pk
  INNER JOIN hx.account_ident ai ON ci.account_pk = ai.pk
  WHERE cs.epoch BETWEEN epoch_start AND epoch_end
    AND ci.kind = 'Client'
    AND cs.rtt > p_rtt_ms * 1000000
    AND cs.stop_time = '0001-01-01T00:00:00Z';
