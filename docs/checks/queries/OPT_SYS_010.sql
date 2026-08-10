CREATE OR REPLACE MACRO audit.check_opt_sys_010(epoch_ts, range_start, p_max_ipq := 1000) AS TABLE
  SELECT
    'OPT_SYS_010' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END || ' / ' || rg.group_name AS entity,
    rg.server_pk AS entity_pk,
    GREATEST(rg.ipq_prop_len, rg.ipq_entry_len, rg.ipq_resp_len, rg.ipq_apply_len) AS max_ipq_len,
    rg.ipq_prop_len,
    rg.ipq_entry_len,
    rg.ipq_resp_len,
    rg.ipq_apply_len,
    rg.group_name,
    epoch_ts AS epoch
  FROM hx.raft_groups rg
  INNER JOIN hx.server_ident srv ON rg.server_pk = srv.pk
  WHERE rg.epoch = epoch_ts
    AND GREATEST(rg.ipq_prop_len, rg.ipq_entry_len, rg.ipq_resp_len, rg.ipq_apply_len) > p_max_ipq;
