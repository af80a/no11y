CREATE OR REPLACE MACRO audit.check_opt_place_002(epoch_ts, range_start, p_min_conns := 5, p_min_dominance_pct := 60.0) AS TABLE
  WITH consumer_leaders AS (
    SELECT
      cr.consumer_pk,
      cr.server_pk AS leader_server_pk
    FROM hx.consumer_replica_stats cr
    WHERE cr.epoch = epoch_ts
      AND cr.is_leader = true
  ),
  consumer_accounts AS (
    SELECT
      cl.consumer_pk,
      cl.leader_server_pk,
      ci.name AS consumer_name,
      sti.name AS stream_name,
      sti.account_pk
    FROM consumer_leaders cl
    INNER JOIN hx.consumer_ident ci ON cl.consumer_pk = ci.pk
    INNER JOIN hx.stream_ident sti ON ci.stream_pk = sti.pk
  ),
  conn_clusters AS (
    SELECT
      cni.account_pk,
      si.cluster,
      count(*) AS conn_count
    FROM hx.conn_ident cni
    INNER JOIN hx.conn_stats cs ON cni.pk = cs.conn_pk
    INNER JOIN hx.server_ident si ON cni.server_pk = si.pk
    WHERE cs.epoch = epoch_ts
      AND cni.kind = 'Client'
      AND cs.stop_time = '0001-01-01T00:00:00Z'
      AND cni.account_pk IS NOT NULL
      AND si.cluster IS NOT NULL AND si.cluster <> ''
    GROUP BY cni.account_pk, si.cluster
  ),
  account_totals AS (
    SELECT account_pk, sum(conn_count) AS total
    FROM conn_clusters
    GROUP BY account_pk
  ),
  dominant_cluster AS (
    SELECT DISTINCT ON (cc.account_pk)
      cc.account_pk,
      cc.cluster AS dominant_cluster,
      cc.conn_count AS dominant_conn_count,
      tot.total AS total_conns
    FROM conn_clusters cc
    INNER JOIN account_totals tot ON cc.account_pk = tot.account_pk
    ORDER BY cc.account_pk, cc.conn_count DESC
  )
  SELECT
    'OPT_PLACE_002' AS code,
    ai.name || ' / ' || ca.stream_name || ' / ' || ca.consumer_name AS entity,
    ca.consumer_pk AS entity_pk,
    ls.cluster AS leader_cluster,
    dc.dominant_cluster AS dominant_cluster,
    dc.dominant_conn_count AS dominant_conn_count,
    round(dc.dominant_conn_count * 100.0 / dc.total_conns, 1) AS dominant_conn_pct,
    epoch_ts AS epoch
  FROM consumer_accounts ca
  INNER JOIN hx.server_ident ls ON ca.leader_server_pk = ls.pk
  INNER JOIN dominant_cluster dc ON ca.account_pk = dc.account_pk
  INNER JOIN hx.account_ident ai ON ca.account_pk = ai.pk
  WHERE ls.cluster IS NOT NULL AND ls.cluster <> ''
    AND ls.cluster != dc.dominant_cluster
    AND dc.total_conns >= p_min_conns
    AND dc.dominant_conn_count * 100.0 / dc.total_conns >= p_min_dominance_pct;
