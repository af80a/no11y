CREATE OR REPLACE MACRO audit.check_server_012(epoch_start, epoch_end) AS TABLE
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
  deltas AS (
    SELECT ep.epoch, c.server_pk, GREATEST(c.stale_connections - p.stale_connections, 0) AS delta
    FROM epoch_pairs ep
    INNER JOIN hx.server_stats c ON c.epoch = ep.epoch
    INNER JOIN hx.server_stats p ON c.server_pk = p.server_pk AND p.epoch = ep.prev_epoch
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  )
  SELECT
    'SERVER_012' AS code,
    CASE WHEN i.cluster IS NOT NULL AND i.cluster != '' THEN i.cluster || ' / ' || i.name ELSE i.name END AS entity,
    i.pk AS entity_pk,
    d.delta AS stale_connections,
    d.epoch
  FROM deltas d
  INNER JOIN hx.server_ident i ON d.server_pk = i.pk
  WHERE d.delta > 0;
