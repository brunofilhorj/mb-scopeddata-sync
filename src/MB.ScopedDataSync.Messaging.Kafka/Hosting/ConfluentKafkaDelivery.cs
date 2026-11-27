using Confluent.Kafka;

namespace MB.ScopedDataSync.Messaging.Kafka.Hosting;

internal sealed class ConfluentKafkaDelivery(IConsumer<string, string> consumer) : IKafkaDelivery
{
    public ConsumeResult<string, string>? Consume(CancellationToken cancellationToken)
        => consumer.Consume(cancellationToken);

    public ConsumeResult<string, string>? Consume(TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return consumer.Consume(timeout);
    }

    public void Commit(ConsumeResult<string, string> result) => consumer.Commit(result);

    public void Pause(TopicPartition partition) => consumer.Pause([partition]);

    public void Resume(TopicPartition partition) => consumer.Resume([partition]);

    public void Seek(TopicPartitionOffset offset) => consumer.Seek(offset);
}
