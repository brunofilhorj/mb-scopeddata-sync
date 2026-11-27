namespace MB.ScopedDataSync.Producer.Publishing;

public sealed record PublishEntityEventRequest<TPayload>(
    string EntityId,
    string Scope,
    string Topic,
    TPayload? Payload,
    DateTimeOffset? OccurredAtUtc = null);
