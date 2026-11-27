namespace MB.ScopedDataSync.Abstractions.Alerts;

public sealed record StaleRecordAlert(
    string EntityId,
    string Scope,
    long? Version,
    DateTimeOffset? UpdatedAtUtc,
    TimeSpan Age,
    int ConsecutiveFailures,
    DateTimeOffset ObservedAtUtc);
