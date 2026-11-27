using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace MB.ScopedDataSync.Messaging.Kafka.Hosting;

/// <summary>
/// Auto-commit fica desligado. Offset só avança depois de <see cref="KafkaCommitDecision.Commit"/>.
/// <see cref="KafkaCommitDecision.Retry"/> pausa a partição e relê a mesma mensagem depois do atraso.
/// As outras partições continuam. Falha de consume ou commit não encerra o loop.
/// </summary>
internal sealed class KafkaDeliveryPump(
    ILogger logger,
    IScopedDataSyncErrorHandler errors,
    TimeProvider time,
    TimeSpan retryDelay)
{
    public async Task RunAsync(
        IKafkaDelivery delivery,
        Func<string, string?, CancellationToken, Task<KafkaCommitDecision>> dispatch,
        CancellationToken cancellationToken)
    {
        var held = new List<HeldPartition>();
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await ReleaseReadyAsync(delivery, held, cancellationToken).ConfigureAwait(false);
                ConsumeResult<string, string>? result;
                try
                {
                    var wait = NextHold(held);
                    result = wait is null
                        ? delivery.Consume(cancellationToken)
                        : delivery.Consume(wait.Value, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    await ReportFaultAsync(string.Empty, null, "kafka.consume", exception, cancellationToken)
                        .ConfigureAwait(false);
                    await DelayAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (result?.Message is null)
                {
                    continue;
                }

                KafkaCommitDecision decision;
                try
                {
                    decision = await dispatch(result.Topic, result.Message.Value, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    await ReportFaultAsync(string.Empty, null, "kafka.dispatch", exception, cancellationToken)
                        .ConfigureAwait(false);
                    Hold(delivery, held, result);
                    continue;
                }

                if (decision == KafkaCommitDecision.Commit)
                {
                    try
                    {
                        delivery.Commit(result);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception exception)
                    {
                        await ReportFaultAsync(string.Empty, null, "kafka.commit", exception, cancellationToken)
                            .ConfigureAwait(false);
                        Hold(delivery, held, result);
                    }

                    continue;
                }

                Hold(delivery, held, result);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task ReleaseReadyAsync(
        IKafkaDelivery delivery,
        List<HeldPartition> held,
        CancellationToken cancellationToken)
    {
        var now = time.GetUtcNow();
        for (var index = held.Count - 1; index >= 0; index--)
        {
            if (held[index].ResumeAt > now)
            {
                continue;
            }

            var ready = held[index];
            held.RemoveAt(index);
            try
            {
                delivery.Resume(ready.Partition);
                delivery.Seek(ready.Offset);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                held.Add(ready with { ResumeAt = time.GetUtcNow().Add(retryDelay) });
                await ReportFaultAsync(string.Empty, null, "kafka.resume", exception, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private TimeSpan? NextHold(List<HeldPartition> held)
    {
        if (held.Count == 0)
        {
            return null;
        }

        var next = held.Min(item => item.ResumeAt);
        var remaining = next - time.GetUtcNow();
        return remaining <= TimeSpan.Zero ? TimeSpan.Zero : remaining;
    }

    private void Hold(IKafkaDelivery delivery, List<HeldPartition> held, ConsumeResult<string, string> result)
    {
        if (held.Any(item => Equals(item.Partition, result.TopicPartition)))
        {
            return;
        }

        delivery.Pause(result.TopicPartition);
        held.Add(new HeldPartition(result.TopicPartition, result.TopicPartitionOffset, time.GetUtcNow().Add(retryDelay)));
    }

    private async Task DelayAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(retryDelay, time, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task ReportFaultAsync(
        string scope,
        string? entityId,
        string operation,
        Exception exception,
        CancellationToken cancellationToken)
    {
        logger.LogError(exception, "Kafka consumer {Operation} failed. The loop continues.", operation);
        await ScopedDataSyncErrorNotifications.NotifyAsync(
            errors,
            logger,
            new ScopedDataSyncFailure(
                ScopedDataSyncFailureKind.KafkaConsumerFaulted,
                scope,
                entityId,
                operation,
                exception),
            cancellationToken).ConfigureAwait(false);
    }

    private sealed record HeldPartition(TopicPartition Partition, TopicPartitionOffset Offset, DateTimeOffset ResumeAt);
}
