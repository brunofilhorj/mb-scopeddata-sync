namespace MB.ScopedDataSync.Abstractions.Projection;

public enum ProjectionApplyOutcome
{
    Applied = 0,
    DiscardedStaleOrDuplicate = 1
}
