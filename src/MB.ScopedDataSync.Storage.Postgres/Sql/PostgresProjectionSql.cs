namespace MB.ScopedDataSync.Storage.Postgres.Sql;

/// <summary>
/// SQL encapsulado da projeção. A cerca monotônica é
/// <c>WHERE entity_scope_projections.version &lt; EXCLUDED.version</c>.
/// O registro de falha não lista <c>version</c> nem <c>payload</c> no <c>SET</c>.
/// </summary>
public static class PostgresProjectionSql
{
    public const string TableName = "entity_scope_projections";

    public const string EnsureSchema = """
        CREATE TABLE IF NOT EXISTS entity_scope_projections (
            entity_id text NOT NULL,
            scope text NOT NULL,
            version bigint NOT NULL,
            payload jsonb NULL,
            updated_at_utc timestamptz NULL,
            consecutive_failures integer NOT NULL DEFAULT 0,
            last_failure_at_utc timestamptz NULL,
            last_failure_reason text NULL,
            CONSTRAINT pk_entity_scope_projections PRIMARY KEY (entity_id, scope),
            CONSTRAINT ck_entity_scope_projections_version CHECK (version >= 0)
        );
        """;

    public const string TryApply = """
        WITH attempted AS (
            INSERT INTO entity_scope_projections (
                entity_id,
                scope,
                version,
                payload,
                updated_at_utc,
                consecutive_failures,
                last_failure_at_utc,
                last_failure_reason)
            VALUES (
                @entity_id,
                @scope,
                @version,
                @payload,
                @updated_at_utc,
                0,
                NULL,
                NULL)
            ON CONFLICT (entity_id, scope)
            DO UPDATE SET
                version = EXCLUDED.version,
                payload = EXCLUDED.payload,
                updated_at_utc = EXCLUDED.updated_at_utc,
                consecutive_failures = 0,
                last_failure_at_utc = NULL,
                last_failure_reason = NULL
            WHERE entity_scope_projections.version < EXCLUDED.version
            RETURNING version
        )
        SELECT version, TRUE AS applied FROM attempted
        UNION ALL
        SELECT version, FALSE AS applied
        FROM entity_scope_projections
        WHERE entity_id = @entity_id
          AND scope = @scope
          AND NOT EXISTS (SELECT 1 FROM attempted);
        """;

    public const string RecordFailure = """
        INSERT INTO entity_scope_projections (
            entity_id,
            scope,
            version,
            payload,
            updated_at_utc,
            consecutive_failures,
            last_failure_at_utc,
            last_failure_reason)
        VALUES (
            @entity_id,
            @scope,
            0,
            NULL,
            NULL,
            1,
            @failed_at_utc,
            @reason)
        ON CONFLICT (entity_id, scope)
        DO UPDATE SET
            consecutive_failures = entity_scope_projections.consecutive_failures + 1,
            last_failure_at_utc = EXCLUDED.last_failure_at_utc,
            last_failure_reason = EXCLUDED.last_failure_reason;
        """;

    public const string Read = """
        SELECT version,
               payload,
               updated_at_utc,
               consecutive_failures,
               last_failure_at_utc,
               last_failure_reason
        FROM entity_scope_projections
        WHERE entity_id = @entity_id
          AND scope = @scope;
        """;
}
