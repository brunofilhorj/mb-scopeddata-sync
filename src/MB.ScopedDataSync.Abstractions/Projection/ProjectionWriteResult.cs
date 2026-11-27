namespace MB.ScopedDataSync.Abstractions.Projection;

public readonly record struct ProjectionWriteResult(bool Applied, long CurrentVersion);
