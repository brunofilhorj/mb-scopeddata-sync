namespace MB.ScopedDataSync.Abstractions.Projection;

public sealed record ProjectionApplyResult(
    ProjectionApplyOutcome Outcome,
    long IncomingVersion,
    long CurrentVersion);
