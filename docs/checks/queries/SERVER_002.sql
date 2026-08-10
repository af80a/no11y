CREATE OR REPLACE MACRO audit.check_server_002(epoch_start, epoch_end) AS TABLE
  WITH epoch_versions AS (
    SELECT
      epoch,
      COALESCE(NULLIF(trim(cluster), ''), '') AS cluster_key,
      version,
      COUNT(DISTINCT pk) AS cnt
    FROM hx.servers
    WHERE epoch BETWEEN epoch_start AND epoch_end
    GROUP BY epoch, cluster_key, version
  ),
  majority AS (
    SELECT epoch, cluster_key, version
    FROM (
      -- Tie-break with version DESC so a 50/50 split resolves deterministically rather than by
      -- arbitrary row order. For the common dotted-numeric form with equal segment widths this
      -- picks the newer string as the majority and flags the older servers; the only case it gets
      -- "older" wrong is an exact even split across versions of differing widths (e.g. 2.9 vs
      -- 2.10), which lexicographic compare can't order â determinism is the guarantee that matters.
      SELECT epoch, cluster_key, version, cnt,
        ROW_NUMBER() OVER (PARTITION BY epoch, cluster_key ORDER BY cnt DESC, version DESC) AS rn
      FROM epoch_versions
    )
    WHERE rn = 1
  )
  SELECT
    'SERVER_002' AS code,
    CASE WHEN s.cluster IS NOT NULL AND s.cluster != '' THEN s.cluster || ' / ' || s.name ELSE s.name END AS entity,
    s.pk AS entity_pk,
    s.version AS old_value,
    m.version AS new_value,
    s.epoch
  FROM hx.servers s
  INNER JOIN majority m
    ON s.epoch = m.epoch
    AND COALESCE(NULLIF(trim(s.cluster), ''), '') = m.cluster_key
  WHERE s.epoch BETWEEN epoch_start AND epoch_end
    AND s.version != m.version;
