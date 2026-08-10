CREATE OR REPLACE MACRO audit.check_leaf_002(epoch_start, epoch_end, p_rtt_ms := 100.0) AS TABLE
  SELECT
    'LEAF_002' AS code,
    srv.name || ' / ' || l.name AS entity,
    l.pk AS entity_pk,
    round(l.rtt / 1000000.0, 1) AS rtt_ms,
    l.epoch
  FROM hx.leafs l
  INNER JOIN hx.server_ident srv ON l.server_pk = srv.pk
  WHERE l.epoch BETWEEN epoch_start AND epoch_end
    AND l.rtt > p_rtt_ms * 1000000;
