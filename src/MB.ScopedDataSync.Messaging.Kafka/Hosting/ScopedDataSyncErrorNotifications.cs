using Microsoft.Extensions.Logging;

namespace MB.ScopedDataSync.Messaging.Kafka.Hosting;

internal static class ScopedDataSyncErrorNotifications
{
    public static async Task NotifyAsync(
        IScopedDataSyncErrorHandler handler,
        ILogger logger,
        ScopedDataSyncFailure failure,
        CancellationToken cancellationToken)
    {
        try
        {
            await handler.HandleAsync(failure, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            logger.LogWarning(exception, "Scoped data sync error handler failed for {Operation}.", failure.Operation);
        }
    }
}
