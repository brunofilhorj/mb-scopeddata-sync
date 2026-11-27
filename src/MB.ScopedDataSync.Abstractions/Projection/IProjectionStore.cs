namespace MB.ScopedDataSync.Abstractions.Projection;

/// <summary>
/// Ponto de extensão dos provedores de cache e dos stores de banco, relacional ou NoSQL.
/// <see cref="TryApplyAsync"/> compara e grava <c>Version</c> no mesmo passo atômico:
/// a escrita ocorre somente quando <paramref name="version"/> é maior que a versão armazenada.
/// <see cref="RecordFailureAsync"/> não pode reduzir <c>Version</c> nem trocar o payload aceito.
/// </summary>
public interface IProjectionStore<TPayload>
{
    Task<ProjectionRecord<TPayload>?> GetAsync(string entityId, CancellationToken cancellationToken);

    Task<ProjectionWriteResult> TryApplyAsync(
        string entityId,
        long version,
        TPayload payload,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken);

    Task RecordFailureAsync(
        string entityId,
        DateTimeOffset failedAtUtc,
        string reason,
        CancellationToken cancellationToken);
}
