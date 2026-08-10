CREATE OR REPLACE MACRO audit.check_opt_sys_014(epoch_ts, range_start, p_pending_mib := 1.0) AS TABLE
  SELECT
    'OPT_SYS_014' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    g.server_pk AS entity_pk,
    rsrv.pk AS remote_pk,
    rsrv.name AS remote_name,
    round(g.pending_size / 1048576.0, 1) AS pending_mib,
    epoch_ts AS epoch
  FROM hx.gateways g
  INNER JOIN hx.server_ident srv ON g.server_pk = srv.pk
  INNER JOIN hx.server_ident rsrv ON g.remote_server_pk = rsrv.pk
  WHERE g.epoch = epoch_ts
    AND g.pending_size > p_pending_mib * 1048576;
