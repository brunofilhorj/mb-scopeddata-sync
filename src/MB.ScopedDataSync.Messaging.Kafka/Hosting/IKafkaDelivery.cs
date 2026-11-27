using Confluent.Kafka;

namespace MB.ScopedDataSync.Messaging.Kafka.Hosting;

internal interface IKafkaDelivery
{
    ConsumeResult<string, string>? Consume(CancellationToken cancellationToken);

    ConsumeResult<string, string>? Consume(TimeSpan timeout, CancellationToken cancellationToken);

    void Commit(ConsumeResult<string, string> result);

    void Pause(TopicPartition partition);

    void Resume(TopicPartition partition);

    void Seek(TopicPartitionOffset offset);
}
