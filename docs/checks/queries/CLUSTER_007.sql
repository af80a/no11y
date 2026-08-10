CREATE OR REPLACE MACRO audit.check_cluster_007(epoch_start, epoch_end) AS TABLE
  WITH prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.server_stats WHERE epoch < epoch_start
  ),
  range_epochs AS (
    SELECT epoch FROM prev_epoch WHERE epoch IS NOT NULL
    UNION
    SELECT DISTINCT epoch FROM hx.server_stats WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  epoch_pairs AS (
    SELECT epoch, LAG(epoch) OVER (ORDER BY epoch) AS prev_epoch FROM range_epochs
  ),
  -- Per-epoch set of logical links: one row per (epoch, server, normalized
  -- remote cluster), collapsing the per-connection gateway_pk grain.
  links AS (
    SELECT DISTINCT
      gs.epoch,
      gi.server_pk,
      NULLIF(lower(trim(rs.cluster)), '') AS remote_cluster
    FROM hx.gateway_stats gs
    INNER JOIN hx.gateway_ident gi ON gi.pk = gs.gateway_pk
    INNER JOIN hx.server_ident rs ON gi.remote_server_pk = rs.pk
  ),
  lost AS (
    SELECT
      ep.epoch,
      p.server_pk,
      p.remote_cluster
    FROM epoch_pairs ep
    INNER JOIN links p ON p.epoch = ep.prev_epoch
    LEFT JOIN links c
      ON c.epoch = ep.epoch
      AND c.server_pk = p.server_pk
      AND c.remote_cluster IS NOT DISTINCT FROM p.remote_cluster
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
      AND c.server_pk IS NULL
  )
  SELECT
    'CLUSTER_007' AS code,
    CASE WHEN si.cluster IS NOT NULL AND si.cluster != '' THEN si.cluster || ' / ' || si.name ELSE si.name END AS entity,
    si.pk AS entity_pk,
    l.remote_cluster AS remote_cluster,
    l.epoch
  FROM lost l
  INNER JOIN hx.server_ident si ON l.server_pk = si.pk;
