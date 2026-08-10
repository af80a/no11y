CREATE OR REPLACE MACRO audit.check_jetstream_005(epoch_start, epoch_end, p_max_pending := 1000) AS TABLE
  SELECT
    'JETSTREAM_005' AS code,
    CASE WHEN cluster IS NOT NULL AND cluster != '' THEN cluster || ' / ' || name ELSE name END AS entity,
    pk AS entity_pk,
    js_api_inflight,
    epoch
  FROM hx.servers
  WHERE epoch BETWEEN epoch_start AND epoch_end
    AND js_api_inflight > p_max_pending;
