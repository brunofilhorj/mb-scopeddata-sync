using System.Text.Json;
using MB.ScopedDataSync.Storage.Postgres.Sql;
using Npgsql;
using NpgsqlTypes;

namespace MB.ScopedDataSync.Storage.Postgres.Store;

/// <summary>
/// Store PostgreSQL da projeção. O upsert condicional e a leitura do resultado andam no mesmo statement,
/// então a decisão <c>version</c> maior não depende de um select separado.
/// </summary>
public sealed class PostgresProjectionStore<TPayload> : IProjectionStore<TPayload>
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly string _scope;

    public PostgresProjectionStore(NpgsqlDataSource dataSource, string scope)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        SyncNameRules.ThrowIfInvalidScope(scope);
        _dataSource = dataSource;
        _scope = scope;
    }

    public async Task<ProjectionRecord<TPayload>?> GetAsync(string entityId, CancellationToken cancellationToken)
    {
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        await using var command = _dataSource.CreateCommand(PostgresProjectionSql.Read);
        command.Parameters.AddWithValue("entity_id", entityId);
        command.Parameters.AddWithValue("scope", _scope);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return ReadRecord(reader);
    }

    public async Task<ProjectionWriteResult> TryApplyAsync(
        string entityId,
        long version,
        TPayload payload,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken)
    {
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        ArgumentNullException.ThrowIfNull(payload);
        if (version <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(version));
        }

        await using var command = _dataSource.CreateCommand(PostgresProjectionSql.TryApply);
        command.Parameters.AddWithValue("entity_id", entityId);
        command.Parameters.AddWithValue("scope", _scope);
        command.Parameters.AddWithValue("version", version);
        command.Parameters.Add(new NpgsqlParameter("payload", NpgsqlDbType.Jsonb)
        {
            Value = JsonSerializer.Serialize(payload, ScopedDataSyncJson.Options)
        });
        command.Parameters.AddWithValue("updated_at_utc", updatedAtUtc.ToUniversalTime());

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidOperationException("The projection upsert did not return a version decision.");
        }

        var currentVersion = reader.GetInt64(0);
        var applied = reader.GetBoolean(1);
        return new ProjectionWriteResult(applied, currentVersion);
    }

    public async Task RecordFailureAsync(
        string entityId,
        DateTimeOffset failedAtUtc,
        string reason,
        CancellationToken cancellationToken)
    {
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        await using var command = _dataSource.CreateCommand(PostgresProjectionSql.RecordFailure);
        command.Parameters.AddWithValue("entity_id", entityId);
        command.Parameters.AddWithValue("scope", _scope);
        command.Parameters.AddWithValue("failed_at_utc", failedAtUtc.ToUniversalTime());
        command.Parameters.AddWithValue("reason", reason);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static ProjectionRecord<TPayload> ReadRecord(NpgsqlDataReader reader)
    {
        var version = reader.GetInt64(0);
        var payloadText = ReadJson(reader, 1);
        TPayload? payload = default;
        var hasPayload = false;
        if (!string.IsNullOrEmpty(payloadText))
        {
            payload = JsonSerializer.Deserialize<TPayload>(payloadText, ScopedDataSyncJson.Options);
            hasPayload = payload is not null;
        }

        return new ProjectionRecord<TPayload>(
            version,
            payload,
            hasPayload,
            reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2),
            reader.GetInt32(3),
            reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4),
            reader.IsDBNull(5) ? null : reader.GetString(5));
    }

    private static string? ReadJson(NpgsqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            return null;
        }

        var value = reader.GetValue(ordinal);
        if (value is JsonDocument document)
        {
            using (document)
            {
                return document.RootElement.GetRawText();
            }
        }

        return value switch
        {
            string text => text,
            JsonElement element => element.GetRawText(),
            _ => value.ToString()
        };
    }
}
