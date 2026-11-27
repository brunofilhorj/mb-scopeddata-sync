namespace MB.ScopedDataSync.Consumer.Projection;

internal sealed class ScopeReadGateway<TPayload>(ProjectionCoordinator<TPayload> coordinator)
    : IScopeReader<TPayload>
{
    public Task<ReadResult<TPayload>> ReadAsync(string entityId, CancellationToken cancellationToken = default)
        => coordinator.ReadAsync(entityId, cancellationToken);
}
