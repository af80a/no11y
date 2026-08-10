CREATE OR REPLACE MACRO audit.check_meta_009(epoch_start, epoch_end) AS TABLE
  WITH per_epoch AS (
    SELECT
      epoch,
      COUNT(DISTINCT COALESCE(NULLIF(peer_server_pk, 0), server_pk)) AS peer_count
    FROM hx.meta_cluster_stats
    WHERE epoch BETWEEN epoch_start AND epoch_end
    GROUP BY epoch
  ),
  prev_epoch AS (
    SELECT MAX(epoch) AS epoch FROM hx.meta_cluster_stats WHERE epoch < epoch_start
  ),
  prev_count AS (
    -- NULL (via NULLIF on the empty-set COUNT of 0) when no epoch precedes
    -- the range, so the first epoch ever has no baseline and cannot fire.
    SELECT NULLIF(COUNT(DISTINCT COALESCE(NULLIF(peer_server_pk, 0), server_pk)), 0) AS peer_count
    FROM hx.meta_cluster_stats
    WHERE epoch = (SELECT epoch FROM prev_epoch)
  ),
  with_prev AS (
    SELECT
      pe.epoch,
      pe.peer_count AS current_size,
      COALESCE(LAG(pe.peer_count) OVER (ORDER BY pe.epoch), pc.peer_count) AS prev_size
    FROM per_epoch pe
    CROSS JOIN prev_count pc
  )
  SELECT
    'META_009' AS code,
    'meta cluster' AS entity,
    0 AS entity_pk,
    prev_size,
    current_size,
    epoch
  FROM with_prev
  WHERE prev_size IS NOT NULL
    AND current_size < prev_size;
