CREATE OR REPLACE MACRO audit.check_change_002(epoch_start, epoch_end) AS TABLE
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
    SELECT ep.epoch, o.server_pk, o.js_domain
    FROM epoch_pairs ep
    INNER JOIN hx.server_opts o ON o.epoch = ep.epoch
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  ),
  prev_opts AS (
    SELECT ep.epoch, o.server_pk, o.js_domain
    FROM epoch_pairs ep
    INNER JOIN hx.server_opts o ON o.epoch = ep.prev_epoch
    WHERE ep.prev_epoch IS NOT NULL
  )
  SELECT
    'CHANGE_002' AS code,
    CASE WHEN i.cluster IS NOT NULL AND i.cluster != '' THEN i.cluster || ' / ' || i.name ELSE i.name END AS entity,
    c.server_pk AS entity_pk,
    CASE WHEN p.js_domain = '' THEN '(empty)' ELSE p.js_domain END AS old_value,
    CASE WHEN c.js_domain = '' THEN '(empty)' ELSE c.js_domain END AS new_value,
    c.epoch
  FROM current_opts c
  INNER JOIN prev_opts p ON c.server_pk = p.server_pk AND c.epoch = p.epoch
  INNER JOIN hx.server_ident i ON i.pk = c.server_pk
  WHERE c.js_domain != p.js_domain;
