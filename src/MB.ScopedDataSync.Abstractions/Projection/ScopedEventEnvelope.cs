namespace MB.ScopedDataSync.Abstractions.Projection;

public sealed record ScopedEventEnvelope<TPayload>(
    string EntityId,
    string Scope,
    long Version,
    TPayload Payload,
    DateTimeOffset OccurredAtUtc);
