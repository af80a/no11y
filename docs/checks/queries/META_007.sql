CREATE OR REPLACE MACRO audit.check_meta_007(epoch_start, epoch_end) AS TABLE
  WITH per_epoch AS (
    SELECT
      epoch,
      MAX(cluster_size) AS cluster_size
    FROM hx.meta_cluster_stats
    WHERE epoch BETWEEN epoch_start AND epoch_end
    GROUP BY epoch
  )
  SELECT
    'META_007' AS code,
    'meta cluster' AS entity,
    0 AS entity_pk,
    cluster_size,
    epoch
  FROM per_epoch
  WHERE cluster_size > 1
    AND cluster_size % 2 = 0;
