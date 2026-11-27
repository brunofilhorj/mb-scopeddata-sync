namespace MB.ScopedDataSync.Producer.Publishing;

public sealed record PublishedScopedEvent(
    Guid EventId,
    string EntityId,
    string KafkaKey,
    string Scope,
    string Topic,
    long Version,
    string PayloadJson,
    DateTimeOffset OccurredAtUtc);
