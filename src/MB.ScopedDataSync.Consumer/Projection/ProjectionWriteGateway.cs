namespace MB.ScopedDataSync.Consumer.Projection;

[EventConsumerOnly]
internal sealed class ProjectionWriteGateway<TPayload>(ProjectionCoordinator<TPayload> coordinator)
    : IProjectionUpdater<TPayload>
{
    public Task<ProjectionApplyResult> ApplyEventAsync(
        ScopedEventEnvelope<TPayload> envelope,
        CancellationToken cancellationToken = default)
        => coordinator.ApplyAsync(envelope, cancellationToken);

    public Task RecordSyncFailureAsync(SyncFailure failure, CancellationToken cancellationToken = default)
        => coordinator.RecordFailureAsync(failure, cancellationToken);
}
