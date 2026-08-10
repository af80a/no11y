CREATE OR REPLACE MACRO audit.check_server_001(epoch_start, epoch_end) AS TABLE
  SELECT
    'SERVER_001' AS code,
    CASE WHEN i.cluster IS NOT NULL AND i.cluster != '' THEN i.cluster || ' / ' || i.name ELSE i.name END AS entity,
    i.pk AS entity_pk,
    -- Truncate 56-char NKEYs to first 8 chars for readability.
    regexp_replace(
      e.error,
      '([A-Z][A-Z0-9]{7})[A-Z0-9]{48}',
      '\1...',
      'g'
    ) AS error_text,
    e.epoch
  FROM (
    SELECT h.epoch, h.server_pk,
      UNNEST(h.errors) AS error,
      UNNEST(h.error_types) AS error_type
    FROM hx.server_health_stats h
    WHERE h.epoch BETWEEN epoch_start AND epoch_end
      AND h.status != 'ok'
      AND len(h.errors) > 0
  ) e
  JOIN hx.server_ident i ON i.pk = e.server_pk
  WHERE e.error_type = 'CONNECTION'
  UNION ALL
  -- Edge case: unhealthy but no error messages.
  SELECT
    'SERVER_001' AS code,
    CASE WHEN i.cluster IS NOT NULL AND i.cluster != '' THEN i.cluster || ' / ' || i.name ELSE i.name END AS entity,
    i.pk AS entity_pk,
    'Health status: ' || h.status AS error_text,
    h.epoch
  FROM hx.server_health_stats h
  JOIN hx.server_ident i ON i.pk = h.server_pk
  WHERE h.epoch BETWEEN epoch_start AND epoch_end
    AND h.status != 'ok'
    AND len(h.errors) = 0;
