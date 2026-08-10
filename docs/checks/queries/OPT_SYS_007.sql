CREATE OR REPLACE MACRO audit.check_opt_sys_007(epoch_ts, range_start, p_max_lag := 100) AS TABLE
  SELECT
    'OPT_SYS_007' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END || ' / ' || rg.group_name AS entity,
    rg.server_pk AS entity_pk,
    rg.committed - rg.applied AS apply_lag,
    rg.committed,
    rg.applied,
    epoch_ts AS epoch
  FROM hx.raft_groups rg
  INNER JOIN hx.server_ident srv ON rg.server_pk = srv.pk
  WHERE rg.epoch = epoch_ts
    AND rg.catching_up = false
    AND rg.committed - rg.applied > p_max_lag;
