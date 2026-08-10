CREATE OR REPLACE MACRO audit.check_opt_idle_007(epoch_ts, range_start, p_idle_minutes := 5.0) AS TABLE
  SELECT
    'OPT_IDLE_007' AS code,
    ai.name || ' / ' || srv.name || ' / ' || CASE WHEN ci.name != '' THEN ci.name ELSE 'cid:' || ci.id END AS entity,
    ci.pk AS entity_pk,
    round(cs.idle / 60000000000.0, 1) AS idle_minutes,
    epoch_ts AS epoch
  FROM hx.conn_stats cs
  INNER JOIN hx.conn_ident ci ON ci.pk = cs.conn_pk
  INNER JOIN hx.server_ident srv ON ci.server_pk = srv.pk
  INNER JOIN hx.account_ident ai ON ci.account_pk = ai.pk
  WHERE cs.epoch = epoch_ts
    AND ci.kind = 'Client'
    AND cs.stop_time = '0001-01-01T00:00:00Z'
    AND cs.idle > p_idle_minutes * 60000000000
    AND cs.msgs_sent = 0
    AND cs.msgs_recv = 0;
