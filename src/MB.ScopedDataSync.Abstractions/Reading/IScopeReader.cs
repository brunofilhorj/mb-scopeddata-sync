using MB.ScopedDataSync.Abstractions.Projection;

namespace MB.ScopedDataSync.Abstractions.Reading;

/// <summary>
/// Contrato de leitura do domínio. Não expõe atualização de projeção: aplicar evento
/// fica em <see cref="IProjectionUpdater{TPayload}"/>, visível só ao consumidor.
/// </summary>
public interface IScopeReader<TPayload>
{
    Task<ReadResult<TPayload>> ReadAsync(string entityId, CancellationToken cancellationToken = default);
}
