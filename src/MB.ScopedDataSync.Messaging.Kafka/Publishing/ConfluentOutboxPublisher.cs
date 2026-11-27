using System.Text;
using Confluent.Kafka;
using MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;
using MB.ScopedDataSync.Producer.Outbox;

namespace MB.ScopedDataSync.Messaging.Kafka.Publishing;

internal sealed class ConfluentOutboxPublisher : IOutboxKafkaPublisher, IDisposable
{
    private readonly IProducer<string, string> _producer;

    public ConfluentOutboxPublisher(ScopedDataSyncKafkaProducerOptions options)
    {
        _producer = new ProducerBuilder<string, string>(KafkaProducerConfig.Create(options)).Build();
    }

    public async Task PublishAsync(OutboxKafkaMessage message, CancellationToken cancellationToken)
    {
        var kafkaMessage = new Message<string, string>
        {
            Key = message.Key,
            Value = message.Value,
            Headers = []
        };
        foreach (var header in message.Headers)
        {
            kafkaMessage.Headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        }

        await _producer.ProduceAsync(message.Topic, kafkaMessage, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose() => _producer.Dispose();
}
