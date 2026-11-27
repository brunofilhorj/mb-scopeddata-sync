using Confluent.Kafka;

namespace MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;

public sealed class ScopedDataSyncKafkaProducerOptions
{
    public string? BootstrapServers { get; set; }

    public string? ClientId { get; set; }

    /// <summary>
    /// Ajuste de segurança do cluster, como SASL ou SSL.
    /// <c>Acks</c> volta para <c>All</c> depois deste callback.
    /// </summary>
    public Action<ProducerConfig>? Configure { get; set; }
}
