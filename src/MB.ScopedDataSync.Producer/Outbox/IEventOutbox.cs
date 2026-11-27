using MB.ScopedDataSync.Producer.Publishing;

namespace MB.ScopedDataSync.Producer.Outbox;

/// <summary>
/// Incrementa a versão de <c>(entityId, scope)</c> e grava <c>outbox_events</c> na mesma transação.
/// </summary>
public interface IEventOutbox
{
    Task<PublishedScopedEvent> AppendAsync(NewOutboxEvent newEvent, CancellationToken cancellationToken);
}
