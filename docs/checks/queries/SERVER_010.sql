CREATE OR REPLACE MACRO audit.check_server_010(epoch_start, epoch_end, p_rtt_ms := 50.0) AS TABLE
  SELECT
    'SERVER_010' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    r.server_pk AS entity_pk,
    rsrv.pk AS remote_pk,
    rsrv.name AS remote_name,
    round(r.rtt / 1000000.0, 1) AS rtt_ms,
    r.epoch
  FROM hx.routes r
  INNER JOIN hx.server_ident srv ON r.server_pk = srv.pk
  INNER JOIN hx.server_ident rsrv ON r.remote_server_pk = rsrv.pk
  WHERE r.epoch BETWEEN epoch_start AND epoch_end
    AND r.rtt > p_rtt_ms * 1000000;
