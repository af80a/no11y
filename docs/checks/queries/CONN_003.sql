CREATE OR REPLACE MACRO audit.check_conn_003(epoch_start, epoch_end) AS TABLE
  SELECT
    'CONN_003' AS code,
    ai.name || ' / ' || srv.name || ' / ' || CASE WHEN ci.name != '' THEN ci.name ELSE 'cid:' || ci.id END AS entity,
    ci.pk AS entity_pk,
    ci.server_pk AS server_pk,
    srv.name AS server_name,
    cs.reason AS reason,
    cs.epoch
  FROM hx.conn_stats cs
  INNER JOIN hx.conn_ident ci ON ci.pk = cs.conn_pk
  INNER JOIN hx.server_ident srv ON ci.server_pk = srv.pk
  INNER JOIN hx.account_ident ai ON ci.account_pk = ai.pk
  WHERE cs.epoch BETWEEN epoch_start AND epoch_end
    AND cs.stop_time != '0001-01-01T00:00:00Z'
    AND cs.reason != '';
