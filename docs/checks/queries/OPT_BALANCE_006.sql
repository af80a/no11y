CREATE OR REPLACE MACRO audit.check_opt_balance_006(epoch_ts, range_start) AS TABLE
  WITH conn_counts AS (
    SELECT ci.server_pk, ci.account_pk, count(*) AS conns
    FROM hx.conn_ident ci
    INNER JOIN hx.conn_stats cs ON ci.pk = cs.conn_pk
    WHERE cs.epoch = epoch_ts
      AND cs.stop_time = '0001-01-01T00:00:00Z'
      AND ci.account_pk IS NOT NULL
      AND ci.kind = 'Client'
    GROUP BY ci.server_pk, ci.account_pk
  ),
  with_cluster AS (
    SELECT
      cc.server_pk,
      cc.account_pk,
      cc.conns,
      si.name AS server_name,
      si.cluster
    FROM conn_counts cc
    INNER JOIN hx.server_ident si ON cc.server_pk = si.pk
    WHERE si.cluster IS NOT NULL AND si.cluster <> ''
  ),
  account_cluster_totals AS (
    SELECT
      account_pk,
      cluster,
      sum(conns) AS total_conns,
      count(*) AS server_count
    FROM with_cluster
    GROUP BY account_pk, cluster
  )
  SELECT
    'OPT_BALANCE_006' AS code,
    CASE WHEN wc.cluster IS NOT NULL AND wc.cluster != '' THEN wc.cluster || ' / ' || wc.server_name ELSE wc.server_name END AS entity,
    wc.server_pk AS entity_pk,
    round(100.0 * wc.conns / act.total_conns, 0) AS pct,
    ai.pk AS account_pk,
    ai.name AS account_name,
    wc.conns AS conns,
    act.total_conns AS total_conns,
    wc.cluster AS cluster_name,
    epoch_ts AS epoch
  FROM with_cluster wc
  INNER JOIN account_cluster_totals act
    ON wc.account_pk = act.account_pk AND wc.cluster = act.cluster
  INNER JOIN hx.account_ident ai ON wc.account_pk = ai.pk
  WHERE act.server_count >= 3
    AND act.total_conns >= 10
    AND (1.0 * wc.conns / act.total_conns) > 0.7;
