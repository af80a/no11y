CREATE OR REPLACE MACRO audit.check_server_008(epoch_start, epoch_end) AS TABLE
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
  current_servers AS (
    SELECT DISTINCT ep.epoch, i.pk, i.name, i.cluster, i.start_time, i.version
    FROM epoch_pairs ep
    INNER JOIN hx.server_stats s ON s.epoch = ep.epoch
    INNER JOIN hx.server_ident i ON s.server_pk = i.pk
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  ),
  prev_servers AS (
    SELECT DISTINCT ep.epoch, i.name, i.start_time, i.version
    FROM epoch_pairs ep
    INNER JOIN hx.server_stats s ON s.epoch = ep.prev_epoch
    INNER JOIN hx.server_ident i ON s.server_pk = i.pk
    WHERE ep.prev_epoch IS NOT NULL
  )
  SELECT
    'SERVER_008' AS code,
    CASE WHEN c.cluster IS NOT NULL AND c.cluster != '' THEN c.cluster || ' / ' || c.name ELSE c.name END AS entity,
    -- The current (post-restart) pk for the name.
    c.pk AS entity_pk,
    c.epoch
  FROM current_servers c
  INNER JOIN prev_servers p ON c.name = p.name AND c.epoch = p.epoch
  WHERE c.start_time != p.start_time
    AND c.version = p.version;
