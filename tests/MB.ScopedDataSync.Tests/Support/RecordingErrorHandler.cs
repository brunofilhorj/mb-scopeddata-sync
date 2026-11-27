using MB.ScopedDataSync.Abstractions.Errors;

namespace MB.ScopedDataSync.Tests.Support;

internal sealed class RecordingErrorHandler : IScopedDataSyncErrorHandler
{
    public List<ScopedDataSyncFailure> Failures { get; } = [];

    public Exception? Throw { get; set; }

    public Task HandleAsync(ScopedDataSyncFailure failure, CancellationToken cancellationToken = default)
    {
        Failures.Add(failure);
        if (Throw is not null)
        {
            throw Throw;
        }

        return Task.CompletedTask;
    }
}
