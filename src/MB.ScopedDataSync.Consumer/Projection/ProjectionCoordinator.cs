using MB.ScopedDataSync.Consumer.Staleness;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MB.ScopedDataSync.Consumer.Projection;

/// <summary>
/// Orquestra o cache escolhido e o store de banco opcional.
/// A cerca de versão do banco decide o apply; o cache só é atualizado depois dessa gravação.
/// Leitura quente usa o cache. Miss ou payload expirado consulta o banco e reidrata o cache.
/// Alerta de staleness passa pelo <see cref="IAlertCooldownGate"/> por <c>scope:entityId</c>.
/// </summary>
internal sealed class ProjectionCoordinator<TPayload>
{
    private const int MaxFailureReasonLength = 1024;

    private readonly string _scope;
    private readonly StalenessOptions _staleness;
    private readonly IProjectionStore<TPayload>? _cache;
    private readonly IProjectionStore<TPayload>? _database;
    private readonly IAlertCooldownGate _cooldown;
    private readonly IStaleAlertSink _alerts;
    private readonly IScopedDataSyncErrorHandler _errors;
    private readonly IScopedDataSyncInstrumentation _instrumentation;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;

    public ProjectionCoordinator(
        string scope,
        StalenessOptions staleness,
        IProjectionStore<TPayload>? cache,
        IProjectionStore<TPayload>? database,
        IAlertCooldownGate cooldown,
        IStaleAlertSink alerts,
        IScopedDataSyncErrorHandler errors,
        IScopedDataSyncInstrumentation instrumentation,
        TimeProvider time,
        ILogger logger)
    {
        _scope = scope;
        _staleness = staleness;
        _cache = cache;
        _database = database;
        _cooldown = cooldown;
        _alerts = alerts;
        _errors = errors;
        _instrumentation = instrumentation;
        _time = time;
        _logger = logger;
    }

    public async Task<ReadResult<TPayload>> ReadAsync(string entityId, CancellationToken cancellationToken)
    {
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        using var track = _instrumentation.Track("entity.projection.read", _scope);
        try
        {
            var record = await ReadCacheAsync(entityId, cancellationToken).ConfigureAwait(false);

            if ((record is null || !record.HasPayload) && _database is not null)
            {
                var databaseRecord = await _database.GetAsync(entityId, cancellationToken).ConfigureAwait(false);
                if (databaseRecord is not null)
                {
                    if (_cache is not null && databaseRecord.HasPayload && databaseRecord.Payload is not null && databaseRecord.UpdatedAtUtc is not null)
                    {
                        try
                        {
                            await _cache.TryApplyAsync(
                                entityId,
                                databaseRecord.Version,
                                databaseRecord.Payload,
                                databaseRecord.UpdatedAtUtc.Value,
                                cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception exception) when (exception is not OperationCanceledException)
                        {
                            _logger.LogWarning(
                                exception,
                                "Projection {Scope} for {EntityId} was read from the database store and the cache backfill failed.",
                                _scope,
                                entityId);
                        }
                    }

                    record = databaseRecord;
                }
            }

            var now = _time.GetUtcNow();
            var freshness = StalenessEvaluator.Evaluate(record, _staleness, now);
            if (freshness.Status == DataFreshnessStatus.Stale)
            {
                freshness = await PublishStaleAlertAsync(entityId, freshness, now, cancellationToken).ConfigureAwait(false);
            }

            track.SetOutcome(freshness.Status.ToString());
            _instrumentation.ProjectionRead(_scope, freshness.Status, freshness.Age?.TotalMilliseconds);
            var data = record is { HasPayload: true } ? record.Payload : default;
            return new ReadResult<TPayload>(data, freshness);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            track.SetError(exception);
            throw;
        }
    }

    private async Task<ProjectionRecord<TPayload>?> ReadCacheAsync(string entityId, CancellationToken cancellationToken)
    {
        if (_cache is null)
        {
            return null;
        }

        try
        {
            return await _cache.GetAsync(entityId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException && _database is not null)
        {
            _logger.LogWarning(
                exception,
                "Projection {Scope} for {EntityId} was read from the database store because the cache read failed.",
                _scope,
                entityId);
            await NotifyAsync(
                new ScopedDataSyncFailure(
                    ScopedDataSyncFailureKind.CacheDegraded,
                    _scope,
                    entityId,
                    "entity.projection.read",
                    exception),
                cancellationToken).ConfigureAwait(false);
            return null;
        }
    }

    public async Task<ProjectionApplyResult> ApplyAsync(
        ScopedEventEnvelope<TPayload> envelope,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        SyncNameRules.ThrowIfInvalidEntityId(envelope.EntityId);
        if (!string.Equals(envelope.Scope, _scope, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"The event scope '{envelope.Scope}' does not match the registered scope '{_scope}'.");
        }

        if (envelope.Version <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(envelope),
                envelope.Version,
                "Version must be greater than zero.");
        }

        if (envelope.Payload is null)
        {
            throw new ArgumentNullException(
                nameof(envelope),
                "A null payload is not applied. Publish an empty collection instead of a tombstone.");
        }

        using var track = _instrumentation.Track("entity.projection.apply", _scope);
        try
        {
            var updatedAt = envelope.OccurredAtUtc.ToUniversalTime();

            if (_database is not null)
            {
                var databaseResult = await _database.TryApplyAsync(
                    envelope.EntityId,
                    envelope.Version,
                    envelope.Payload,
                    updatedAt,
                    cancellationToken).ConfigureAwait(false);

                if (!databaseResult.Applied)
                {
                    track.SetOutcome("discarded");
                    _instrumentation.EventDiscarded(_scope);
                    return new ProjectionApplyResult(
                        ProjectionApplyOutcome.DiscardedStaleOrDuplicate,
                        envelope.Version,
                        databaseResult.CurrentVersion);
                }

                await TryUpdateCacheAsync(envelope, updatedAt, cancellationToken).ConfigureAwait(false);
                track.SetOutcome("applied");
                _instrumentation.EventApplied(_scope);
                return new ProjectionApplyResult(
                    ProjectionApplyOutcome.Applied,
                    envelope.Version,
                    databaseResult.CurrentVersion);
            }

            var cacheResult = await _cache!.TryApplyAsync(
                envelope.EntityId,
                envelope.Version,
                envelope.Payload,
                updatedAt,
                cancellationToken).ConfigureAwait(false);

            if (!cacheResult.Applied)
            {
                track.SetOutcome("discarded");
                _instrumentation.EventDiscarded(_scope);
                return new ProjectionApplyResult(
                    ProjectionApplyOutcome.DiscardedStaleOrDuplicate,
                    envelope.Version,
                    cacheResult.CurrentVersion);
            }

            track.SetOutcome("applied");
            _instrumentation.EventApplied(_scope);
            return new ProjectionApplyResult(
                ProjectionApplyOutcome.Applied,
                envelope.Version,
                cacheResult.CurrentVersion);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            track.SetError(exception);
            throw;
        }
    }

    public async Task RecordFailureAsync(SyncFailure failure, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(failure);
        SyncNameRules.ThrowIfInvalidEntityId(failure.EntityId);
        if (string.IsNullOrWhiteSpace(failure.Reason))
        {
            throw new ArgumentException("A sync failure reason is required.", nameof(failure));
        }

        var reason = failure.Reason.Trim();
        if (reason.Length > MaxFailureReasonLength)
        {
            reason = reason[..MaxFailureReasonLength];
        }

        using var track = _instrumentation.Track("entity.projection.sync_failure", _scope);
        try
        {
            var failedAt = failure.FailedAtUtc.ToUniversalTime();

            if (_database is not null)
            {
                await _database.RecordFailureAsync(failure.EntityId, failedAt, reason, cancellationToken).ConfigureAwait(false);
            }

            if (_cache is not null)
            {
                try
                {
                    await _cache.RecordFailureAsync(failure.EntityId, failedAt, reason, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException && _database is not null)
                {
                    _logger.LogWarning(
                        exception,
                        "Sync failure for {Scope} entity {EntityId} was stored in the database and the cache update failed.",
                        _scope,
                        failure.EntityId);
                }
            }

            track.SetOutcome("recorded");
            _instrumentation.SyncFailureRecorded(_scope);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            track.SetError(exception);
            throw;
        }
    }

    private async Task TryUpdateCacheAsync(
        ScopedEventEnvelope<TPayload> envelope,
        DateTimeOffset updatedAt,
        CancellationToken cancellationToken)
    {
        if (_cache is null)
        {
            return;
        }

        try
        {
            await _cache.TryApplyAsync(
                envelope.EntityId,
                envelope.Version,
                envelope.Payload,
                updatedAt,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogWarning(
                exception,
                "Database store projection {Scope} version {Version} for {EntityId} was committed, and the cache update failed.",
                _scope,
                envelope.Version,
                envelope.EntityId);
        }
    }

    private async Task<FreshnessInfo> PublishStaleAlertAsync(
        string entityId,
        FreshnessInfo freshness,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        var key = _scope + ":" + entityId;
        if (!_cooldown.TryEnter(key, _staleness.AlertCooldown, now))
        {
            return freshness with { AlertSuppressedByCooldown = true };
        }

        try
        {
            await _alerts.PublishAsync(
                new StaleRecordAlert(
                    entityId,
                    _scope,
                    freshness.Version,
                    freshness.UpdatedAtUtc,
                    freshness.Age ?? TimeSpan.Zero,
                    freshness.ConsecutiveFailures,
                    now),
                cancellationToken).ConfigureAwait(false);
            _instrumentation.StaleAlertEmitted(_scope);
            return freshness;
        }
        catch (Exception exception)
        {
            _cooldown.Undo(key, now);
            if (exception is OperationCanceledException)
            {
                throw;
            }

            _logger.LogWarning(
                exception,
                "Stale alert for {Scope} entity {EntityId} failed and the cooldown was released.",
                _scope,
                entityId);
            await NotifyAsync(
                new ScopedDataSyncFailure(
                    ScopedDataSyncFailureKind.StaleAlertFailed,
                    _scope,
                    entityId,
                    "entity.projection.stale_alert",
                    exception),
                cancellationToken).ConfigureAwait(false);
            return freshness;
        }
    }

    private async Task NotifyAsync(ScopedDataSyncFailure failure, CancellationToken cancellationToken)
    {
        try
        {
            await _errors.HandleAsync(failure, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _logger.LogWarning(exception, "Scoped data sync error handler failed for {Operation}.", failure.Operation);
        }
    }
}
