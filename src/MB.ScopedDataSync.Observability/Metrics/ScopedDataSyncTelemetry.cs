using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace MB.ScopedDataSync.Observability.Metrics;

public static class ScopedDataSyncTelemetry
{
    public const string Name = "MB.ScopedDataSync";

    public static readonly Meter Meter = new(Name, "1.0.0");

    public static readonly ActivitySource ActivitySource = new(Name, "1.0.0");
}
