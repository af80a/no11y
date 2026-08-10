CREATE OR REPLACE MACRO audit.check_jetstream_004(epoch_start, epoch_end, p_max_rps := 50.0) AS TABLE
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
    SELECT ep.epoch, c.server_pk,
           GREATEST(c.js_api_total - p.js_api_total, 0) AS delta_requests,
           GREATEST(c.js_api_errors - p.js_api_errors, 0) AS error_delta,
           date_diff('second', ep.prev_epoch, ep.epoch) AS interval_secs
    FROM epoch_pairs ep
    INNER JOIN hx.server_stats c ON c.epoch = ep.epoch
    INNER JOIN hx.server_stats p ON c.server_pk = p.server_pk AND p.epoch = ep.prev_epoch
    WHERE ep.prev_epoch IS NOT NULL
      AND ep.epoch BETWEEN epoch_start AND epoch_end
  )
  SELECT
    'JETSTREAM_004' AS code,
    CASE WHEN srv.cluster IS NOT NULL AND srv.cluster != '' THEN srv.cluster || ' / ' || srv.name ELSE srv.name END AS entity,
    d.server_pk AS entity_pk,
    round(d.delta_requests * 1.0 / d.interval_secs, 2) AS rate_rps,
    d.delta_requests,
    d.error_delta,
    d.epoch
  FROM deltas d
  INNER JOIN hx.server_ident srv ON d.server_pk = srv.pk
  WHERE d.interval_secs > 0
    AND d.delta_requests * 1.0 / d.interval_secs >= p_max_rps;
