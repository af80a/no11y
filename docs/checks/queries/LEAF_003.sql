CREATE OR REPLACE MACRO audit.check_leaf_003(epoch_start, epoch_end, p_warn_subs := 20000, p_crit_subs := 50000) AS TABLE
  SELECT
    'LEAF_003' AS code,
    CASE WHEN l.num_subs > p_crit_subs THEN 2 WHEN l.num_subs > p_warn_subs THEN 1 END AS severity,
    srv.name || ' / ' || l.name AS entity,
    l.pk AS entity_pk,
    l.num_subs::BIGINT AS num_subs,
    l.epoch
  FROM hx.leafs l
  INNER JOIN hx.server_ident srv ON l.server_pk = srv.pk
  WHERE l.epoch BETWEEN epoch_start AND epoch_end
    AND l.num_subs > p_warn_subs;
