namespace MB.ScopedDataSync.Producer.Postgres;

/// <summary>
/// SQL do produtor. O incremento usa o row lock do <c>ON CONFLICT DO UPDATE</c>
/// e devolve a nova versão com <c>RETURNING</c>. A inserção da outbox acontece
/// na mesma transação, com <c>kafka_key</c> igual à entidade.
/// </summary>
public static class PostgresOutboxSql
{
    public const string EnsureSchema = """
        CREATE TABLE IF NOT EXISTS entity_scope_versions (
            entity_id text NOT NULL,
            scope text NOT NULL,
            version bigint NOT NULL,
            CONSTRAINT pk_entity_scope_versions PRIMARY KEY (entity_id, scope),
            CONSTRAINT ck_entity_scope_versions_version CHECK (version > 0)
        );

        CREATE TABLE IF NOT EXISTS outbox_events (
            event_id uuid NOT NULL,
            entity_id text NOT NULL,
            kafka_key text NOT NULL,
            scope text NOT NULL,
            topic text NOT NULL,
            version bigint NOT NULL,
            payload jsonb NOT NULL,
            occurred_at_utc timestamptz NOT NULL,
            published_at_utc timestamptz NULL,
            CONSTRAINT pk_outbox_events PRIMARY KEY (event_id),
            CONSTRAINT ck_outbox_events_version CHECK (version > 0)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_outbox_events_scope_version
            ON outbox_events (entity_id, scope, version);

        CREATE INDEX IF NOT EXISTS ix_outbox_events_unpublished
            ON outbox_events (occurred_at_utc)
            WHERE published_at_utc IS NULL;
        """;

    public const string IncrementVersion = """
        INSERT INTO entity_scope_versions (entity_id, scope, version)
        VALUES (@entity_id, @scope, 1)
        ON CONFLICT (entity_id, scope)
        DO UPDATE SET version = entity_scope_versions.version + 1
        RETURNING version;
        """;

    public const string InsertOutbox = """
        INSERT INTO outbox_events (
            event_id,
            entity_id,
            kafka_key,
            scope,
            topic,
            version,
            payload,
            occurred_at_utc)
        VALUES (
            @event_id,
            @entity_id,
            @kafka_key,
            @scope,
            @topic,
            @version,
            @payload,
            @occurred_at_utc);
        """;

    public const string LeaseUnpublished = """
        SELECT event_id,
               entity_id,
               kafka_key,
               scope,
               topic,
               version,
               payload,
               occurred_at_utc
        FROM outbox_events
        WHERE published_at_utc IS NULL
        ORDER BY occurred_at_utc
        LIMIT @batch_size
        FOR UPDATE SKIP LOCKED;
        """;

    public const string MarkPublished = """
        UPDATE outbox_events
        SET published_at_utc = @published_at_utc
        WHERE event_id = @event_id;
        """;
}
