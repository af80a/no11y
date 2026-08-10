CREATE OR REPLACE MACRO audit.check_consumer_004(epoch_start, epoch_end) AS TABLE
  WITH stream_leaders AS (
    SELECT stream_pk, epoch, first_seq
    FROM hx.stream_replica_stats
    WHERE epoch BETWEEN epoch_start AND epoch_end
      AND is_leader = true
  )
  SELECT
    'CONSUMER_004' AS code,
    ai.name || ' / ' || si.name || ' / ' || ci.name AS entity,
    ci.pk AS entity_pk,
    cr.delivered_stream_seq AS delivered_seq,
    sl.first_seq AS stream_first_seq,
    cr.epoch
  FROM hx.consumer_replica_stats cr
  INNER JOIN hx.consumer_ident ci ON ci.pk = cr.consumer_pk
  INNER JOIN hx.stream_ident si ON si.pk = ci.stream_pk
  INNER JOIN hx.account_ident ai ON ai.pk = si.account_pk
  INNER JOIN stream_leaders sl ON sl.stream_pk = ci.stream_pk AND sl.epoch = cr.epoch
  WHERE cr.epoch BETWEEN epoch_start AND epoch_end
    AND cr.is_leader = true
    AND cr.delivered_stream_seq > 0
    AND cr.delivered_stream_seq < sl.first_seq;
