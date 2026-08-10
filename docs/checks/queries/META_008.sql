CREATE OR REPLACE MACRO audit.check_meta_008(epoch_start, epoch_end, p_max_pending := 500) AS TABLE
  WITH leader_pending AS (
    SELECT
      epoch,
      COALESCE(NULLIF(peer_server_pk, 0), server_pk) AS leader_pk,
      MAX(pending) AS pending
    FROM hx.meta_cluster_stats
    WHERE epoch BETWEEN epoch_start AND epoch_end
      AND is_leader = true
    GROUP BY epoch, COALESCE(NULLIF(peer_server_pk, 0), server_pk)
  )
  SELECT
    'META_008' AS code,
    CASE WHEN leader.cluster IS NOT NULL AND leader.cluster != '' THEN leader.cluster || ' / ' || leader.name ELSE leader.name END AS entity,
    leader.pk AS entity_pk,
    lp.pending,
    lp.epoch
  FROM leader_pending lp
  INNER JOIN hx.server_ident leader ON lp.leader_pk = leader.pk
  WHERE lp.pending > p_max_pending;
