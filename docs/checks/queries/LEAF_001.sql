CREATE OR REPLACE MACRO audit.check_leaf_001(epoch_start, epoch_end) AS TABLE
  SELECT
    'LEAF_001' AS code,
    srv.name || ' / ' || l.name AS entity,
    l.pk AS entity_pk,
    l.name AS leaf_name,
    l.epoch
  FROM hx.leafs l
  INNER JOIN hx.server_ident srv ON l.server_pk = srv.pk
  WHERE l.epoch BETWEEN epoch_start AND epoch_end
    AND l.name != ''
    AND regexp_matches(l.name, '\s');
