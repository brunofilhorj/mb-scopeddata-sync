namespace MB.ScopedDataSync.Producer.Publishing;

/// <summary>
/// Publica um evento de escopo. A <c>Version</c> é alocada no PostgreSQL e a linha de outbox
/// entra na mesma transação. A chave Kafka do registro é o <c>entityId</c>.
/// </summary>
public interface IScopedEventPublisher
{
    Task<PublishedScopedEvent> PublishAsync<TPayload>(
        PublishEntityEventRequest<TPayload> request,
        CancellationToken cancellationToken = default);
}
