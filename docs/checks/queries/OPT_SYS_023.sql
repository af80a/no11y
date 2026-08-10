CREATE OR REPLACE MACRO audit.check_opt_sys_023(epoch_ts, range_start, p_warn_bytes_gib := 10.0, p_crit_bytes_gib := 50.0, p_warn_pct := 50.0, p_crit_pct := 80.0) AS TABLE
  SELECT
    'OPT_SYS_023' AS code,
    CASE
      WHEN rg.wal_bytes >= p_crit_bytes_gib * (1024::BIGINT * 1024 * 1024) THEN 2
      WHEN so.js_max_store > 0 AND rg.wal_bytes * 100.0 / so.js_max_store >= p_crit_pct THEN 2
      ELSE 1
    END AS severity,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END || ' / ' || rg.group_name AS entity,
    rg.server_pk AS entity_pk,
    round(rg.wal_bytes / (1024.0 * 1024 * 1024), 2) AS wal_gib,
    rg.group_name,
    CASE WHEN so.js_max_store > 0
      THEN round(rg.wal_bytes * 100.0 / so.js_max_store, 1)
      ELSE NULL END AS pct_of_max_store,
    epoch_ts AS epoch
  FROM hx.raft_groups rg
  INNER JOIN hx.server_ident srv ON rg.server_pk = srv.pk
  INNER JOIN hx.server_opts so ON rg.server_pk = so.server_pk AND so.epoch = (SELECT MAX(epoch) FROM hx.server_opts WHERE server_pk = rg.server_pk AND epoch <= epoch_ts)
  WHERE rg.epoch = epoch_ts
    AND (rg.wal_bytes >= p_warn_bytes_gib * (1024::BIGINT * 1024 * 1024)
      OR (so.js_max_store > 0 AND rg.wal_bytes * 100.0 / so.js_max_store >= p_warn_pct));
