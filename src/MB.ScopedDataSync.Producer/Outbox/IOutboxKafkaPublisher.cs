namespace MB.ScopedDataSync.Producer.Outbox;

/// <summary>
/// Transporte do relay. <c>AddScopedDataSyncKafkaProducer</c> registra o cliente Confluent,
/// com <see cref="OutboxKafkaMessage.Key"/> como chave e sem converter <c>[]</c> em null.
/// Um transporte próprio implementa esta interface e não chama esse registro.
/// </summary>
public interface IOutboxKafkaPublisher
{
    Task PublishAsync(OutboxKafkaMessage message, CancellationToken cancellationToken);
}
