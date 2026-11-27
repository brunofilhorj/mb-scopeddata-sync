using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Publishing;
using Npgsql;
using NpgsqlTypes;

namespace MB.ScopedDataSync.Producer.Postgres;

/// <summary>
/// Aloca <c>Version</c> e insere <c>outbox_events</c> antes do commit.
/// Se a inserção falha, o incremento volta atrás com o rollback da transação.
/// </summary>
public sealed class PostgresEventOutbox(NpgsqlDataSource dataSource) : IEventOutbox
{
    public async Task<PublishedScopedEvent> AppendAsync(NewOutboxEvent newEvent, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(newEvent);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var published = await OutboxTransactionPipeline.ExecuteAsync(
            new NpgsqlOutboxTransaction(connection, transaction),
            newEvent,
            cancellationToken).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return published;
    }

    private sealed class NpgsqlOutboxTransaction(NpgsqlConnection connection, NpgsqlTransaction transaction) : IOutboxTransaction
    {
        public async Task<long> IncrementVersionAsync(string entityId, string scope, CancellationToken cancellationToken)
        {
            await using var command = new NpgsqlCommand(PostgresOutboxSql.IncrementVersion, connection, transaction);
            command.Parameters.AddWithValue("entity_id", entityId);
            command.Parameters.AddWithValue("scope", scope);
            var value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return value is long version
                ? version
                : throw new InvalidOperationException("Version allocation did not return a bigint.");
        }

        public async Task InsertAsync(PublishedScopedEvent published, CancellationToken cancellationToken)
        {
            await using var command = new NpgsqlCommand(PostgresOutboxSql.InsertOutbox, connection, transaction);
            command.Parameters.AddWithValue("event_id", published.EventId);
            command.Parameters.AddWithValue("entity_id", published.EntityId);
            command.Parameters.AddWithValue("kafka_key", published.KafkaKey);
            command.Parameters.AddWithValue("scope", published.Scope);
            command.Parameters.AddWithValue("topic", published.Topic);
            command.Parameters.AddWithValue("version", published.Version);
            command.Parameters.Add(new NpgsqlParameter("payload", NpgsqlDbType.Jsonb) { Value = published.PayloadJson });
            command.Parameters.AddWithValue("occurred_at_utc", published.OccurredAtUtc);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
