using MB.ScopedDataSync.Abstractions.Errors;
using Microsoft.Extensions.Logging;

namespace MB.ScopedDataSync.Producer.Outbox;

/// <summary>
/// Falha de reserva ou de envio não marca <c>published_at_utc</c> e não encerra o processo.
/// O handler recebe <see cref="ScopedDataSyncFailureKind.OutboxRelayFaulted"/>.
/// </summary>
internal sealed class OutboxRelayLoop
{
    public async Task RunAsync(
        Func<CancellationToken, Task<int>> dispatch,
        TimeSpan idleDelay,
        IScopedDataSyncErrorHandler errors,
        ILogger logger,
        TimeProvider time,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            int sent;
            try
            {
                sent = await dispatch(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Outbox relay dispatch failed. The loop continues.");
                if (!await NotifyAsync(errors, logger, exception, cancellationToken).ConfigureAwait(false))
                {
                    break;
                }

                if (!await DelayAsync(idleDelay, time, cancellationToken).ConfigureAwait(false))
                {
                    break;
                }

                continue;
            }

            if (sent == 0 && !await DelayAsync(idleDelay, time, cancellationToken).ConfigureAwait(false))
            {
                break;
            }
        }
    }

    private static async Task<bool> NotifyAsync(
        IScopedDataSyncErrorHandler errors,
        ILogger logger,
        Exception exception,
        CancellationToken cancellationToken)
    {
        try
        {
            await errors.HandleAsync(
                new ScopedDataSyncFailure(
                    ScopedDataSyncFailureKind.OutboxRelayFaulted,
                    string.Empty,
                    null,
                    "outbox.dispatch",
                    exception),
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return false;
        }
        catch (Exception handlerException)
        {
            logger.LogWarning(handlerException, "Scoped data sync error handler failed for {Operation}.", "outbox.dispatch");
            return true;
        }
    }

    private static async Task<bool> DelayAsync(TimeSpan delay, TimeProvider time, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, time, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }
}
