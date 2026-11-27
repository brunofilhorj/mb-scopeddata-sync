namespace MB.ScopedDataSync.Producer.Outbox;

public sealed record NewOutboxEvent(
    Guid EventId,
    string EntityId,
    string Scope,
    string Topic,
    string PayloadJson,
    DateTimeOffset OccurredAtUtc);
