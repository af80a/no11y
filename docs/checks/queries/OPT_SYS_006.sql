CREATE OR REPLACE MACRO audit.check_opt_sys_006(epoch_ts, range_start) AS TABLE
  SELECT
    'OPT_SYS_006' AS code,
    srv.name || ' / ' || l.name AS entity,
    l.pk AS entity_pk,
    epoch_ts AS epoch
  FROM hx.leafs l
  INNER JOIN hx.server_ident srv ON l.server_pk = srv.pk
  WHERE l.epoch = epoch_ts
    AND l.compression = 'off';
