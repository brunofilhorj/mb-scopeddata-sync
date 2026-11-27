using MB.ScopedDataSync.Abstractions.Alerts;

namespace MB.ScopedDataSync.Tests.Support;

internal sealed class RecordingAlertSink : IStaleAlertSink
{
    public List<StaleRecordAlert> Alerts { get; } = [];

    public Task PublishAsync(StaleRecordAlert alert, CancellationToken cancellationToken = default)
    {
        Alerts.Add(alert);
        return Task.CompletedTask;
    }
}
