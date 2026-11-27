using MB.ScopedDataSync.Producer.Publishing;

namespace MB.ScopedDataSync.Producer.Outbox;

/// <summary>
/// Ordem fixa da publicação: alocar <c>Version</c> e, em seguida, inserir a outbox com
/// <c>KafkaKey = entityId</c>. Os dois passos compartilham a transação de quem chama.
/// </summary>
internal static class OutboxTransactionPipeline
{
    public static async Task<PublishedScopedEvent> ExecuteAsync(
        IOutboxTransaction transaction,
        NewOutboxEvent newEvent,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(newEvent);
        var version = await transaction.IncrementVersionAsync(newEvent.EntityId, newEvent.Scope, cancellationToken)
            .ConfigureAwait(false);
        if (version <= 0)
        {
            throw new InvalidOperationException("The outbox version allocator returned a non-positive version.");
        }

        var published = new PublishedScopedEvent(
            newEvent.EventId,
            newEvent.EntityId,
            KafkaKey: newEvent.EntityId,
            newEvent.Scope,
            newEvent.Topic,
            version,
            newEvent.PayloadJson,
            newEvent.OccurredAtUtc);

        await transaction.InsertAsync(published, cancellationToken).ConfigureAwait(false);
        return published;
    }
}
