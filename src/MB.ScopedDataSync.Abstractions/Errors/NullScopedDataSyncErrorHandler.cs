namespace MB.ScopedDataSync.Abstractions.Errors;

public sealed class NullScopedDataSyncErrorHandler : IScopedDataSyncErrorHandler
{
    public static NullScopedDataSyncErrorHandler Instance { get; } = new();

    public Task HandleAsync(ScopedDataSyncFailure failure, CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}
