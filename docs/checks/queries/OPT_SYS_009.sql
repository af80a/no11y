CREATE OR REPLACE MACRO audit.check_opt_sys_009(epoch_ts, range_start) AS TABLE
  SELECT
    'OPT_SYS_009' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END || ' / ' || rg.group_name AS entity,
    rg.server_pk AS entity_pk,
    epoch_ts AS epoch
  FROM hx.raft_groups rg
  INNER JOIN hx.server_ident srv ON rg.server_pk = srv.pk
  WHERE rg.epoch = epoch_ts
    AND rg.leader = ''
    AND rg.ever_had_leader = true;
