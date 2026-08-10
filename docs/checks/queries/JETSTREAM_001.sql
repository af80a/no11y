CREATE OR REPLACE MACRO audit.check_jetstream_001(epoch_start, epoch_end, p_lag_percent := 10.0) AS TABLE
  WITH leaders AS (
    SELECT pk, epoch, last_seq
    FROM hx.streams
    WHERE epoch BETWEEN epoch_start AND epoch_end
      AND is_leader = true
      AND last_seq > 0
  ),
  lagging AS (
    SELECT
      r.name AS stream_name,
      r.pk AS stream_pk,
      ai.name AS account_name,
      srv.name AS replica_server,
      l.last_seq AS leader_seq,
      r.last_seq AS replica_seq,
      r.server_pk AS replica_server_pk,
      r.is_kv,
      r.is_object,
      r.epoch
    FROM hx.streams r
    INNER JOIN leaders l ON r.pk = l.pk AND r.epoch = l.epoch
    INNER JOIN hx.account_ident ai ON r.account_pk = ai.pk
    INNER JOIN hx.server_ident srv ON r.server_pk = srv.pk
    WHERE r.epoch BETWEEN epoch_start AND epoch_end
      AND r.is_leader = false
      AND r.peer_server_pk = 0
      AND r.last_seq > 0
      AND (l.last_seq - r.last_seq) * 100.0 / l.last_seq > p_lag_percent
  )
  SELECT
    'JETSTREAM_001' AS code,
    account_name || ' / ' || stream_name AS entity,
    stream_pk AS entity_pk,
    CASE WHEN is_kv THEN 'kvstore' WHEN is_object THEN 'objectstore' ELSE 'stream' END AS entity_type,
    replica_server_pk,
    replica_server,
    replica_seq,
    leader_seq,
    round((leader_seq - replica_seq) * 100.0 / leader_seq, 1) AS lag_percent,
    epoch
  FROM lagging;
