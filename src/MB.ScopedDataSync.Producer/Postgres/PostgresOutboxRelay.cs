using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Publishing;
using Npgsql;

namespace MB.ScopedDataSync.Producer.Postgres;

/// <summary>
/// Reserva linhas não publicadas com <c>FOR UPDATE SKIP LOCKED</c>, envia ao Kafka e marca
/// <c>published_at_utc</c> antes do commit. Falha no envio desfaz a reserva.
/// O hosted service de <c>AddScopedDataSyncOutboxRelay</c> chama este método em loop.
/// Reentrega é segura porque o consumidor descarta <c>Version</c> repetida.
/// </summary>
public sealed class PostgresOutboxRelay(NpgsqlDataSource dataSource, IOutboxKafkaPublisher publisher, TimeProvider timeProvider)
{
    public async Task<int> DispatchAsync(int batchSize, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var batch = await ReadBatchAsync(connection, transaction, batchSize, cancellationToken).ConfigureAwait(false);
        var publishedAt = timeProvider.GetUtcNow();
        foreach (var item in batch)
        {
            await publisher.PublishAsync(OutboxKafkaMessage.Create(item), cancellationToken).ConfigureAwait(false);
            await MarkPublishedAsync(connection, transaction, item.EventId, publishedAt, cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return batch.Count;
    }

    private static async Task<List<PublishedScopedEvent>> ReadBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int batchSize,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(PostgresOutboxSql.LeaseUnpublished, connection, transaction);
        command.Parameters.AddWithValue("batch_size", batchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var batch = new List<PublishedScopedEvent>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            batch.Add(new PublishedScopedEvent(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetInt64(5),
                reader.GetString(6),
                reader.GetFieldValue<DateTimeOffset>(7)));
        }

        return batch;
    }

    private static async Task MarkPublishedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        Guid eventId,
        DateTimeOffset publishedAt,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(PostgresOutboxSql.MarkPublished, connection, transaction);
        command.Parameters.AddWithValue("event_id", eventId);
        command.Parameters.AddWithValue("published_at_utc", publishedAt);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
