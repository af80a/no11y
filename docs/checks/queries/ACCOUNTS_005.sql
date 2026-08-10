CREATE OR REPLACE MACRO audit.check_accounts_005(epoch_start, epoch_end) AS TABLE
  WITH
    active_imports AS (
      SELECT ai.*, act.name AS account_name
      FROM hx.account_import_opts ai
      JOIN hx.account_ident act ON act.pk = ai.account_pk
      WHERE ai.invalid = false
        AND ai.epoch = (
          SELECT MAX(epoch) FROM hx.account_import_opts
          WHERE account_pk = ai.account_pk AND epoch <= epoch_end
        )
        AND ai.epoch <= epoch_end
    ),
    non_system AS (
      SELECT ao.account_pk
      FROM hx.account_opts ao
      WHERE ao.is_system = false
        AND ao.epoch = (
          SELECT MAX(epoch) FROM hx.account_opts
          WHERE account_pk = ao.account_pk AND epoch <= epoch_end
        )
    ),
    active_conns AS (
      SELECT c.pk, c.account_pk
      FROM hx.conns c
      WHERE c.epoch BETWEEN epoch_start AND epoch_end
        AND c.stop_time = '0001-01-01T00:00:00Z'
    ),
    -- Distinct subjects with interest per account, split once per row. Empty
    -- subjects can never overlap (subjects_overlap's guards), so dropping them
    -- here leaves the anti-join unchanged.
    account_subs AS (
      SELECT subject, account_pk, string_split(subject, '.') AS tokens
      FROM (
        SELECT DISTINCT s.subject, c.account_pk
        FROM hx.subs s
        JOIN active_conns c ON c.pk = s.conn_pk
        WHERE s.epoch BETWEEN epoch_start AND epoch_end
      )
      WHERE subject IS NOT NULL AND len(subject) > 0
    ),
    -- The subject clients must show interest in: the local alias when the
    -- import remaps, the source subject otherwise. Split once per row.
    import_targets AS (
      SELECT ai.*,
        CASE WHEN ai.local_subject != '' THEN ai.local_subject ELSE ai.subject END AS target_subject,
        string_split(CASE WHEN ai.local_subject != '' THEN ai.local_subject ELSE ai.subject END, '.') AS target_tokens
      FROM active_imports ai
    )
  SELECT
    'ACCOUNTS_005' AS code,
    ai.account_name AS entity,
    ai.account_pk AS entity_pk,
    ai.subject,
    ai.account AS source_account,
    CASE WHEN ai.local_subject != '' THEN ai.local_subject ELSE '' END AS local_subject,
    epoch_end AS epoch
  FROM import_targets ai
  WHERE ai.account_pk IN (SELECT account_pk FROM non_system)
    AND NOT EXISTS (
      SELECT 1 FROM account_subs s
      WHERE s.account_pk = ai.account_pk
        -- An empty/NULL target never overlaps, firing the check regardless of
        -- interest â same as subjects_overlap's guards.
        AND ai.target_subject IS NOT NULL AND len(ai.target_subject) > 0
        AND (
          s.subject = ai.target_subject
          OR s.subject = '>'
          OR ai.target_subject = '>'
          OR (
            -- First-token compatibility: redundant with position 1 of the
            -- token comparison, but prunes most pairs before it runs.
            (s.tokens[1] = ai.target_tokens[1]
             OR s.tokens[1] IN ('*', '>')
             OR ai.target_tokens[1] IN ('*', '>'))
            AND audit.subject_tokens_overlap(s.tokens, ai.target_tokens)
          )
        )
    );
