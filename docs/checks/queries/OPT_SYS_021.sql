CREATE OR REPLACE MACRO audit.check_opt_sys_021(epoch_ts, range_start) AS TABLE
  WITH cluster_sizes AS (
    SELECT NULLIF(trim(srv.cluster), '') AS cluster, COUNT(DISTINCT srv.pk) AS node_count
    FROM hx.server_ident srv
    INNER JOIN hx.server_stats ss ON srv.pk = ss.server_pk AND ss.epoch = epoch_ts
    WHERE NULLIF(trim(srv.cluster), '') IS NOT NULL
    GROUP BY 1
    HAVING COUNT(DISTINCT srv.pk) > 1
  )
  SELECT
    'OPT_SYS_021' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    cs.node_count::BIGINT AS cluster_nodes,
    epoch_ts AS epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  INNER JOIN hx.server_ident srv ON s.server_pk = srv.pk
  INNER JOIN cluster_sizes cs ON NULLIF(trim(srv.cluster), '') = cs.cluster
  WHERE s.epoch = epoch_ts
    AND s.is_leader = true
    AND s.num_replicas = 1
    AND s.is_mirror = false;
