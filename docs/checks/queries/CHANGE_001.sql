CREATE OR REPLACE MACRO audit.check_change_001(epoch_start, epoch_end) AS TABLE
  WITH prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.server_opts WHERE epoch < epoch_start
  ),
  range_epochs AS (
    SELECT epoch FROM prev_epoch WHERE epoch IS NOT NULL
    UNION
    SELECT DISTINCT epoch FROM hx.server_opts WHERE epoch BETWEEN epoch_start AND epoch_end
  ),
  epoch_pairs AS (
    SELECT epoch, LAG(epoch) OVER (ORDER BY epoch) AS prev_epoch FROM range_epochs
  ),
  current_opts AS (
    SELECT ep.epoch, o.server_pk, o.config_load_time
    FROM epoch_pairs ep
    INNER JOIN hx.server_opts o ON o.epoch = ep.epoch
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  ),
  prev_opts AS (
    SELECT ep.epoch, o.server_pk, o.config_load_time
    FROM epoch_pairs ep
    INNER JOIN hx.server_opts o ON o.epoch = ep.prev_epoch
    WHERE ep.prev_epoch IS NOT NULL
  )
  SELECT
    'CHANGE_001' AS code,
    CASE WHEN i.cluster IS NOT NULL AND i.cluster != '' THEN i.cluster || ' / ' || i.name ELSE i.name END AS entity,
    c.server_pk AS entity_pk,
    CAST(p.config_load_time AS VARCHAR) AS old_value,
    CAST(c.config_load_time AS VARCHAR) AS new_value,
    c.epoch
  FROM current_opts c
  INNER JOIN prev_opts p ON c.server_pk = p.server_pk AND c.epoch = p.epoch
  INNER JOIN hx.server_ident i ON i.pk = c.server_pk
  WHERE c.config_load_time != p.config_load_time;
