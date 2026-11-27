using Confluent.Kafka;
using MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;

namespace MB.ScopedDataSync.Messaging.Kafka.Publishing;

internal static class KafkaProducerConfig
{
    public static ProducerConfig Create(ScopedDataSyncKafkaProducerOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = options.ClientId
        };
        options.Configure?.Invoke(config);
        config.Acks = Acks.All;
        return config;
    }
}
