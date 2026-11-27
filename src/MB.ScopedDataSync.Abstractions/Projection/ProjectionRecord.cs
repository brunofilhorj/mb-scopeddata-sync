namespace MB.ScopedDataSync.Abstractions.Projection;

public sealed record ProjectionRecord<TPayload>(
    long Version,
    TPayload? Payload,
    bool HasPayload,
    DateTimeOffset? UpdatedAtUtc,
    int ConsecutiveFailures,
    DateTimeOffset? LastFailureAtUtc,
    string? LastFailureReason);
