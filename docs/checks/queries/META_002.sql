CREATE OR REPLACE MACRO audit.check_meta_002(epoch_start, epoch_end) AS TABLE
  WITH per_epoch_leaders AS (
    SELECT epoch, COUNT(DISTINCT COALESCE(NULLIF(peer_server_pk, 0), server_pk)) AS leader_count
    FROM hx.meta_cluster_stats
    WHERE epoch BETWEEN epoch_start AND epoch_end
      AND is_leader = true
    GROUP BY epoch
  )
  SELECT
    'META_002' AS code,
    'meta cluster' AS entity,
    0 AS entity_pk,
    leader_count,
    epoch
  FROM per_epoch_leaders
  WHERE leader_count > 1;
