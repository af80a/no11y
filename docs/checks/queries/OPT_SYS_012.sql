CREATE OR REPLACE MACRO audit.check_opt_sys_012(epoch_ts, range_start, p_max_churn := 10000) AS TABLE
  WITH prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.server_sublist_stats WHERE epoch < epoch_ts
  ),
  deltas AS (
    SELECT
      c.server_pk,
      GREATEST((c.num_inserts + c.num_removes) - (p.num_inserts + p.num_removes), 0) AS churn
    FROM hx.server_sublist_stats c
    INNER JOIN prev_epoch pe ON pe.epoch IS NOT NULL
    INNER JOIN hx.server_sublist_stats p ON c.server_pk = p.server_pk AND p.epoch = pe.epoch
    WHERE c.epoch = epoch_ts
  )
  SELECT
    'OPT_SYS_012' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    d.server_pk AS entity_pk,
    d.churn,
    epoch_ts AS epoch
  FROM deltas d
  INNER JOIN hx.server_ident srv ON d.server_pk = srv.pk
  WHERE d.churn > p_max_churn;
