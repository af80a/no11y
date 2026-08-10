CREATE OR REPLACE MACRO audit.check_meta_006(epoch_start, epoch_end) AS TABLE
  WITH per_peer AS (
    -- Collapse the (reporter x peer) fan-out to one authoritative row per
    -- peer. peer_server_pk = 0 is the leader's self-report.
    SELECT
      epoch,
      COALESCE(NULLIF(peer_server_pk, 0), server_pk) AS peer_pk,
      cluster_size,
      is_offline
    FROM hx.meta_cluster_stats
    WHERE epoch BETWEEN epoch_start AND epoch_end
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY epoch, COALESCE(NULLIF(peer_server_pk, 0), server_pk)
      -- is_offline ASC: when reporters disagree about a peer, any reporter
      -- still seeing it online wins, so a lone dissenting reporter cannot
      -- mark a peer offline. server_pk DESC (reporter PK) is a final
      -- deterministic tiebreaker so the result never depends on scan order.
      ORDER BY is_leader DESC, is_offline ASC, peer_server_pk DESC, server_pk DESC
    ) = 1
  ),
  per_epoch AS (
    SELECT
      epoch,
      MAX(cluster_size) AS cluster_size,
      COUNT(*) FILTER (WHERE is_offline = true) AS offline_count
    FROM per_peer
    GROUP BY epoch
  )
  SELECT
    'META_006' AS code,
    'meta cluster' AS entity,
    0 AS entity_pk,
    offline_count,
    cluster_size,
    cluster_size // 2 + 1 AS quorum_needed,
    epoch
  FROM per_epoch
  WHERE offline_count * 2 > cluster_size;
