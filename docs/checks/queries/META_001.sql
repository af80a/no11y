CREATE OR REPLACE MACRO audit.check_meta_001(epoch_start, epoch_end) AS TABLE
  WITH per_peer AS (
    -- Collapse the (reporter x peer) fan-out to one authoritative row per
    -- peer. peer_server_pk = 0 is the leader's self-report.
    SELECT
      epoch,
      COALESCE(NULLIF(peer_server_pk, 0), server_pk) AS peer_pk,
      server_pk AS reporter_pk,
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
  )
  SELECT
    'META_001' AS code,
    peer.name AS entity,
    peer.pk AS entity_pk,
    reporter.pk AS reporter_pk,
    reporter.name AS reporter_name,
    pp.epoch
  FROM per_peer pp
  INNER JOIN hx.server_ident reporter ON pp.reporter_pk = reporter.pk
  INNER JOIN hx.server_ident peer ON pp.peer_pk = peer.pk
  WHERE pp.is_offline = true;
