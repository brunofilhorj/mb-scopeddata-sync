using System.Text.Json;
using MB.ScopedDataSync.Abstractions.Projection;
using MB.ScopedDataSync.Abstractions.Serialization;
using MB.ScopedDataSync.Producer.Outbox;

namespace MB.ScopedDataSync.InMemoryExample.Messaging;

internal static class KafkaEnvelopeReader
{
    public static ScopedEventEnvelope<TPayload> Read<TPayload>(OutboxKafkaMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        using var document = JsonDocument.Parse(message.Value);
        var root = document.RootElement;
        var payload = root.GetProperty("payload").Deserialize<TPayload>(ScopedDataSyncJson.Options);
        if (payload is null)
        {
            throw new InvalidOperationException("The Kafka payload must be present. An empty collection is [].");
        }

        var entityId = root.GetProperty("entityId").GetString();
        var scope = root.GetProperty("scope").GetString();
        if (string.IsNullOrWhiteSpace(entityId) || string.IsNullOrWhiteSpace(scope))
        {
            throw new InvalidOperationException("The Kafka value must carry entityId and scope.");
        }

        return new ScopedEventEnvelope<TPayload>(
            entityId,
            scope,
            root.GetProperty("version").GetInt64(),
            payload,
            root.GetProperty("occurredAtUtc").GetDateTimeOffset());
    }
}
