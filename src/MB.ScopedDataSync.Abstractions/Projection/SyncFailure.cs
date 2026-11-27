namespace MB.ScopedDataSync.Abstractions.Projection;

public sealed record SyncFailure(string EntityId, string Reason, DateTimeOffset FailedAtUtc);
