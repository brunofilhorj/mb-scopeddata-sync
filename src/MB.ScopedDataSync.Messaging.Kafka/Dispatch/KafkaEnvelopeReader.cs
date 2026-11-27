using System.Text.Json;

namespace MB.ScopedDataSync.Messaging.Kafka.Dispatch;

/// <summary>
/// Lê o valor publicado pelo produtor: <c>entityId</c>, <c>scope</c>, <c>version</c>, <c>occurredAtUtc</c> e <c>payload</c>.
/// Coleção vazia permanece <c>[]</c>. JSON <c>null</c> não vira snapshot.
/// </summary>
internal static class KafkaEnvelopeReader
{
    public static ScopedEventEnvelope<TPayload> Read<TPayload>(string value)
    {
        using var document = JsonDocument.Parse(value);
        var root = document.RootElement;
        var payload = root.GetProperty("payload").Deserialize<TPayload>(ScopedDataSyncJson.Options);
        if (payload is null)
        {
            throw new InvalidOperationException("The Kafka payload must be present. An empty collection is [].");
        }

        var entityId = root.GetProperty("entityId").GetString();
        var scope = root.GetProperty("scope").GetString();
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        SyncNameRules.ThrowIfInvalidScope(scope);

        return new ScopedEventEnvelope<TPayload>(
            entityId!,
            scope!,
            root.GetProperty("version").GetInt64(),
            payload,
            root.GetProperty("occurredAtUtc").GetDateTimeOffset());
    }

    public static string? TryReadEntityId(string value)
    {
        try
        {
            using var document = JsonDocument.Parse(value);
            if (!document.RootElement.TryGetProperty("entityId", out var property))
            {
                return null;
            }

            var entityId = property.GetString();
            SyncNameRules.ThrowIfInvalidEntityId(entityId);
            return entityId;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return null;
        }
    }
}
