namespace MB.ScopedDataSync.Abstractions.Alerts;

public sealed class NullStaleAlertSink : IStaleAlertSink
{
    public static NullStaleAlertSink Instance { get; } = new();

    public Task PublishAsync(StaleRecordAlert alert, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}
