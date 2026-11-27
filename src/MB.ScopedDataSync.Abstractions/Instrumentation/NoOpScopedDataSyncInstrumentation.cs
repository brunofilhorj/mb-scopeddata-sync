using MB.ScopedDataSync.Abstractions.Reading;

namespace MB.ScopedDataSync.Abstractions.Instrumentation;

public sealed class NoOpScopedDataSyncInstrumentation : IScopedDataSyncInstrumentation
{
    public static NoOpScopedDataSyncInstrumentation Instance { get; } = new();

    public InstrumentationLease Track(string operationName, string scope) => default;

    public void EventApplied(string scope)
    {
    }

    public void EventDiscarded(string scope)
    {
    }

    public void SyncFailureRecorded(string scope)
    {
    }

    public void StaleAlertEmitted(string scope)
    {
    }

    public void ProjectionRead(string scope, DataFreshnessStatus status, double? ageMilliseconds)
    {
    }
}
