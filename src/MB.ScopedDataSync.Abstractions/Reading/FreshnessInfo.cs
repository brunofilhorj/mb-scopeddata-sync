namespace MB.ScopedDataSync.Abstractions.Reading;

public sealed record FreshnessInfo(
    DataFreshnessStatus Status,
    long? Version,
    DateTimeOffset? UpdatedAtUtc,
    TimeSpan? Age,
    int ConsecutiveFailures,
    bool AlertSuppressedByCooldown);
