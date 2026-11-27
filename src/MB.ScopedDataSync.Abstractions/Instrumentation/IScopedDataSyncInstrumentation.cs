using MB.ScopedDataSync.Abstractions.Reading;

namespace MB.ScopedDataSync.Abstractions.Instrumentation;

/// <summary>
/// Gancho opcional de métricas e tracing. A implementação padrão não observa nada;
/// <c>MB.ScopedDataSync.Observability</c> substitui este contrato.
/// </summary>
public interface IScopedDataSyncInstrumentation
{
    InstrumentationLease Track(string operationName, string scope);

    void EventApplied(string scope);

    void EventDiscarded(string scope);

    void SyncFailureRecorded(string scope);

    void StaleAlertEmitted(string scope);

    void ProjectionRead(string scope, DataFreshnessStatus status, double? ageMilliseconds);
}
