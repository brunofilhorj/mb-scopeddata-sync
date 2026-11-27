namespace MB.ScopedDataSync.Abstractions.Projection;

/// <summary>
/// Atualização da projeção para uso exclusivo do consumidor de eventos.
/// <see cref="ApplyEventAsync"/> aceita o envelope somente quando <c>incoming.Version</c>
/// é estritamente maior que a versão persistida. Versão igual ou menor é descarte idempotente.
/// <see cref="RecordSyncFailureAsync"/> incrementa falhas consecutivas sem reduzir <c>Version</c>
/// e sem substituir o payload já aceito.
/// </summary>
[EventConsumerOnly]
public interface IProjectionUpdater<TPayload>
{
    Task<ProjectionApplyResult> ApplyEventAsync(
        ScopedEventEnvelope<TPayload> envelope,
        CancellationToken cancellationToken = default);

    Task RecordSyncFailureAsync(SyncFailure failure, CancellationToken cancellationToken = default);
}
