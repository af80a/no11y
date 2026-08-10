CREATE OR REPLACE MACRO audit.check_server_018(epoch_start, epoch_end, p_rtt_ms := 50.0) AS TABLE
  SELECT
    'SERVER_018' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    g.server_pk AS entity_pk,
    rsrv.pk AS remote_pk,
    rsrv.name AS remote_name,
    round(g.rtt / 1000000.0, 1) AS rtt_ms,
    g.epoch
  FROM hx.gateways g
  INNER JOIN hx.server_ident srv ON g.server_pk = srv.pk
  INNER JOIN hx.server_ident rsrv ON g.remote_server_pk = rsrv.pk
  WHERE g.epoch BETWEEN epoch_start AND epoch_end
    AND g.rtt > p_rtt_ms * 1000000;
