using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace MB.ScopedDataSync.Observability.Metrics;

/// <summary>
/// Métricas e spans de staleness, falha de sync e evento aplicado ou descartado.
/// Tags usam só <c>scope</c> e status, sem <c>entityId</c>.
/// </summary>
public sealed class MetricsScopedDataSyncInstrumentation : IScopedDataSyncInstrumentation
{
    private static readonly Counter<long> Applied = ScopedDataSyncTelemetry.Meter.CreateCounter<long>(
        "mb.scoped_data_sync.events.applied");

    private static readonly Counter<long> Discarded = ScopedDataSyncTelemetry.Meter.CreateCounter<long>(
        "mb.scoped_data_sync.events.discarded");

    private static readonly Counter<long> Failures = ScopedDataSyncTelemetry.Meter.CreateCounter<long>(
        "mb.scoped_data_sync.sync.failures");

    private static readonly Counter<long> StaleAlerts = ScopedDataSyncTelemetry.Meter.CreateCounter<long>(
        "mb.scoped_data_sync.staleness.alerts");

    private static readonly Histogram<double> Age = ScopedDataSyncTelemetry.Meter.CreateHistogram<double>(
        "mb.scoped_data_sync.projection.age",
        unit: "ms",
        advice: new InstrumentAdvice<double>
        {
            HistogramBucketBoundaries =
            [
                100,
                500,
                1_000,
                5_000,
                15_000,
                30_000,
                60_000,
                120_000,
                300_000,
                900_000,
                3_600_000,
                21_600_000,
                86_400_000,
            ],
        });

    public InstrumentationLease Track(string operationName, string scope)
    {
        var activity = ScopedDataSyncTelemetry.ActivitySource.StartActivity(operationName, ActivityKind.Internal);
        activity?.SetTag("entity.scope", scope);
        return new InstrumentationLease(activity);
    }

    public void EventApplied(string scope) => Applied.Add(1, Scope(scope));

    public void EventDiscarded(string scope) => Discarded.Add(1, Scope(scope));

    public void SyncFailureRecorded(string scope) => Failures.Add(1, Scope(scope));

    public void StaleAlertEmitted(string scope) => StaleAlerts.Add(1, Scope(scope));

    public void ProjectionRead(string scope, DataFreshnessStatus status, double? ageMilliseconds)
    {
        if (ageMilliseconds is null)
        {
            return;
        }

        Age.Record(ageMilliseconds.Value, new TagList
        {
            { "scope", scope },
            { "status", status.ToString() }
        });
    }

    private static TagList Scope(string scope) => new() { { "scope", scope } };
}
