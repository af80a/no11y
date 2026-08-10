CREATE OR REPLACE MACRO audit.check_opt_place_001(epoch_ts, range_start, p_min_conn_pct := 10.0, p_min_conn_count := 5) AS TABLE
  WITH conn_clusters AS (
    SELECT
      ci.account_pk,
      si.cluster,
      count(*) AS conn_count
    FROM hx.conn_ident ci
    INNER JOIN hx.conn_stats cs ON ci.pk = cs.conn_pk
    INNER JOIN hx.server_ident si ON ci.server_pk = si.pk
    WHERE cs.epoch = epoch_ts
      AND ci.kind = 'Client'
      AND cs.stop_time = '0001-01-01T00:00:00Z'
      AND ci.account_pk IS NOT NULL
      AND si.cluster IS NOT NULL AND si.cluster <> ''
    GROUP BY ci.account_pk, si.cluster
  ),
  stream_clusters AS (
    SELECT DISTINCT
      sti.account_pk,
      si.cluster
    FROM hx.stream_replica_stats srs
    INNER JOIN hx.stream_ident sti ON srs.stream_pk = sti.pk
    INNER JOIN hx.server_ident si ON srs.server_pk = si.pk
    WHERE srs.epoch = epoch_ts
      AND srs.is_leader = true
      AND si.cluster IS NOT NULL AND si.cluster <> ''
  ),
  total_conns AS (
    SELECT account_pk, sum(conn_count) AS total
    FROM conn_clusters
    GROUP BY account_pk
  ),
  mismatched AS (
    SELECT
      cc.account_pk,
      cc.cluster AS conn_cluster,
      cc.conn_count,
      tc.total
    FROM conn_clusters cc
    INNER JOIN total_conns tc ON cc.account_pk = tc.account_pk
    LEFT JOIN stream_clusters sc
      ON cc.account_pk = sc.account_pk AND cc.cluster = sc.cluster
    WHERE sc.cluster IS NULL
      AND EXISTS (
        SELECT 1 FROM stream_clusters sc2
        WHERE sc2.account_pk = cc.account_pk
      )
  )
  SELECT
    'OPT_PLACE_001' AS code,
    ai.name AS entity,
    ai.pk AS entity_pk,
    round(m.conn_count * 100.0 / m.total, 1) AS conn_pct,
    m.conn_cluster AS conn_cluster,
    (
      SELECT string_agg(sc3.cluster, ', ')
      FROM stream_clusters sc3
      WHERE sc3.account_pk = m.account_pk
    ) AS stream_clusters,
    epoch_ts AS epoch
  FROM mismatched m
  INNER JOIN hx.account_ident ai ON m.account_pk = ai.pk
  WHERE m.conn_count >= p_min_conn_count
    AND m.conn_count * 100.0 / m.total >= p_min_conn_pct;
