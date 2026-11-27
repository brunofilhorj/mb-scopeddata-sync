using Confluent.Kafka;
using MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MB.ScopedDataSync.Messaging.Kafka.Hosting;

internal enum KafkaCommitDecision
{
    Commit = 0,
    Retry = 1
}

internal interface IKafkaConsumerLoop
{
    Task RunAsync(
        ScopedDataSyncKafkaOptions options,
        IReadOnlyCollection<string> topics,
        Func<string, string?, CancellationToken, Task<KafkaCommitDecision>> dispatch,
        CancellationToken cancellationToken);
}

internal sealed class ConfluentKafkaConsumerLoop(ILogger logger, IScopedDataSyncErrorHandler errors, TimeProvider time)
    : IKafkaConsumerLoop
{
    private static readonly TimeSpan StoreRetryDelay = TimeSpan.FromSeconds(1);

    public async Task RunAsync(
        ScopedDataSyncKafkaOptions options,
        IReadOnlyCollection<string> topics,
        Func<string, string?, CancellationToken, Task<KafkaCommitDecision>> dispatch,
        CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = options.ClientId,
            GroupId = options.GroupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topics);
        try
        {
            var pump = new KafkaDeliveryPump(logger, errors, time, StoreRetryDelay);
            await pump.RunAsync(new ConfluentKafkaDelivery(consumer), dispatch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            consumer.Close();
        }
    }
}
