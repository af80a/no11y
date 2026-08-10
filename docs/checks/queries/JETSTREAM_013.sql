CREATE OR REPLACE MACRO audit.check_jetstream_013(epoch_start, epoch_end) AS TABLE
  SELECT
    'JETSTREAM_013' AS code,
    ai.name || ' / ' || s.name AS entity,
    s.pk AS entity_pk,
    CASE WHEN s.is_kv THEN 'kvstore' WHEN s.is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    s.num_subjects,
    s.msgs,
    s.epoch
  FROM hx.streams s
  INNER JOIN hx.account_ident ai ON s.account_pk = ai.pk
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.is_leader = true
    AND s.msgs > 0
    AND s.num_subjects > s.msgs;
