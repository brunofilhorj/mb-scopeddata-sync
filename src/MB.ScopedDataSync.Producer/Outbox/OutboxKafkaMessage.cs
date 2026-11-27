using System.Globalization;
using System.Text.Json;
using MB.ScopedDataSync.Producer.Publishing;

namespace MB.ScopedDataSync.Producer.Outbox;

/// <summary>
/// Mensagem pronta para o produtor Kafka. <see cref="Key"/> é o <c>entityId</c>.
/// O campo <c>payload</c> preserva o JSON já normalizado, inclusive <c>[]</c>.
/// </summary>
public sealed record OutboxKafkaMessage(
    string Topic,
    string Key,
    string Value,
    IReadOnlyDictionary<string, string> Headers)
{
    public static OutboxKafkaMessage Create(PublishedScopedEvent published)
    {
        ArgumentNullException.ThrowIfNull(published);
        if (!string.Equals(published.KafkaKey, published.EntityId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The Kafka key must be the entity id.");
        }

        using var payload = JsonDocument.Parse(published.PayloadJson);
        if (payload.RootElement.ValueKind == JsonValueKind.Null)
        {
            throw new InvalidOperationException("Refusing to publish a tombstone. The payload must be [] when a collection is empty.");
        }

        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteString("entityId", published.EntityId);
            writer.WriteString("scope", published.Scope);
            writer.WriteNumber("version", published.Version);
            writer.WriteString("occurredAtUtc", published.OccurredAtUtc.ToUniversalTime());
            writer.WritePropertyName("payload");
            payload.RootElement.WriteTo(writer);
            writer.WriteEndObject();
        }

        var headers = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["entity-id"] = published.EntityId,
            ["scope"] = published.Scope,
            ["version"] = published.Version.ToString(CultureInfo.InvariantCulture)
        };

        return new OutboxKafkaMessage(
            published.Topic,
            published.KafkaKey,
            System.Text.Encoding.UTF8.GetString(stream.ToArray()),
            headers);
    }
}
