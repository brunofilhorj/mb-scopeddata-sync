namespace MB.ScopedDataSync.Abstractions.Reading;

public sealed record ReadResult<TPayload>(TPayload? Data, FreshnessInfo Freshness);
