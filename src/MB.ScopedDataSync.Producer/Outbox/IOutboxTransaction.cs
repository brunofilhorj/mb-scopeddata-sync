using MB.ScopedDataSync.Producer.Publishing;

namespace MB.ScopedDataSync.Producer.Outbox;

/// <summary>
/// Passos executados dentro de uma única transação de banco. A implementação PostgreSQL
/// abre a transação, chama o pipeline e só então faz commit.
/// </summary>
internal interface IOutboxTransaction
{
    Task<long> IncrementVersionAsync(string entityId, string scope, CancellationToken cancellationToken);

    Task InsertAsync(PublishedScopedEvent published, CancellationToken cancellationToken);
}
