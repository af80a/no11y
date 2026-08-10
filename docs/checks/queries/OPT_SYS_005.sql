CREATE OR REPLACE MACRO audit.check_opt_sys_005(epoch_ts, range_start, p_pending_mib := 1.0) AS TABLE
  SELECT
    'OPT_SYS_005' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    r.server_pk AS entity_pk,
    rsrv.pk AS remote_pk,
    rsrv.name AS remote_name,
    round(r.pending_size / 1048576.0, 1) AS pending_mib,
    epoch_ts AS epoch
  FROM hx.routes r
  INNER JOIN hx.server_ident srv ON r.server_pk = srv.pk
  INNER JOIN hx.server_ident rsrv ON r.remote_server_pk = rsrv.pk
  WHERE r.epoch = epoch_ts
    AND r.pending_size > p_pending_mib * 1048576;
