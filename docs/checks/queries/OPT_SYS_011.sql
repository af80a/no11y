CREATE OR REPLACE MACRO audit.check_opt_sys_011(epoch_ts, range_start, p_multiplier := 10) AS TABLE
  SELECT
    'OPT_SYS_011' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    ss.server_pk AS entity_pk,
    ss.max_fanout,
    round(ss.avg_fanout, 1) AS avg_fanout,
    epoch_ts AS epoch
  FROM hx.server_sublist_stats ss
  INNER JOIN hx.server_ident srv ON ss.server_pk = srv.pk
  WHERE ss.epoch = epoch_ts
    AND ss.avg_fanout > 1
    AND ss.max_fanout > p_multiplier * ss.avg_fanout;
