using Confluent.Kafka;
using MB.ScopedDataSync.Abstractions.Errors;
using MB.ScopedDataSync.Messaging.Kafka.Hosting;
using MB.ScopedDataSync.Tests.Support;
using Microsoft.Extensions.Logging.Abstractions;

namespace MB.ScopedDataSync.Tests.Messaging;

public class KafkaDeliveryPumpTests
{
    [Fact]
    public async Task Retry_pauses_one_partition_and_the_other_commits()
    {
        using var cancellation = new CancellationTokenSource();
        var delivery = new ScriptedKafkaDelivery(cancellation);
        delivery.Enqueue(Message("entity.positions", partition: 0, offset: 4));
        delivery.Enqueue(Message("entity.profile", partition: 1, offset: 9));
        var pump = new KafkaDeliveryPump(
            NullLogger.Instance,
            NullScopedDataSyncErrorHandler.Instance,
            TimeProvider.System,
            TimeSpan.Zero);

        await pump.RunAsync(delivery, (topic, _, _) => Task.FromResult(
            topic == "entity.positions" ? KafkaCommitDecision.Retry : KafkaCommitDecision.Commit),
            cancellation.Token);

        Assert.Equal(
            [
                "pause:entity.positions:0",
                "resume:entity.positions:0",
                "seek:entity.positions:0:4",
                "commit:entity.profile:1:9"
            ],
            delivery.Events);
    }

    [Fact]
    public async Task Consume_failure_keeps_the_loop_alive_and_notifies()
    {
        using var cancellation = new CancellationTokenSource();
        var delivery = new ScriptedKafkaDelivery(cancellation);
        delivery.EnqueueThrow(new IOException("broker down"));
        delivery.Enqueue(Message("entity.profile", partition: 1, offset: 9));
        var handler = new RecordingErrorHandler { Throw = new InvalidOperationException("handler down") };
        var pump = new KafkaDeliveryPump(NullLogger.Instance, handler, TimeProvider.System, TimeSpan.Zero);

        await pump.RunAsync(
            delivery,
            (_, _, _) => Task.FromResult(KafkaCommitDecision.Commit),
            cancellation.Token);

        var failure = Assert.Single(handler.Failures);
        Assert.Equal(ScopedDataSyncFailureKind.KafkaConsumerFaulted, failure.Kind);
        Assert.Equal("kafka.consume", failure.Operation);
        Assert.Equal(["commit:entity.profile:1:9"], delivery.Events);
    }

    private static ConsumeResult<string, string> Message(string topic, int partition, long offset)
        => new()
        {
            Topic = topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, string> { Value = "{}" }
        };

    private sealed class ScriptedKafkaDelivery(CancellationTokenSource cancellation) : IKafkaDelivery
    {
        private readonly Queue<Func<ConsumeResult<string, string>?>> _consumes = new();

        public List<string> Events { get; } = [];

        public void Enqueue(ConsumeResult<string, string> result) => _consumes.Enqueue(() => result);

        public void EnqueueThrow(Exception exception) => _consumes.Enqueue(() => throw exception);

        public ConsumeResult<string, string>? Consume(CancellationToken cancellationToken) => Next();

        public ConsumeResult<string, string>? Consume(TimeSpan timeout, CancellationToken cancellationToken) => Next();

        public void Commit(ConsumeResult<string, string> result)
            => Events.Add($"commit:{result.Topic}:{result.Partition.Value}:{result.Offset.Value}");

        public void Pause(TopicPartition partition)
            => Events.Add($"pause:{partition.Topic}:{partition.Partition.Value}");

        public void Resume(TopicPartition partition)
            => Events.Add($"resume:{partition.Topic}:{partition.Partition.Value}");

        public void Seek(TopicPartitionOffset offset)
            => Events.Add($"seek:{offset.Topic}:{offset.Partition.Value}:{offset.Offset.Value}");

        private ConsumeResult<string, string>? Next()
        {
            if (_consumes.Count == 0)
            {
                cancellation.Cancel();
                throw new OperationCanceledException(cancellation.Token);
            }

            return _consumes.Dequeue()();
        }
    }
}
