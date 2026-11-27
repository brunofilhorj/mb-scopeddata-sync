namespace MB.ScopedDataSync.Abstractions.Errors;

/// <summary>
/// Falha já contida pela biblioteca. <see cref="Operation"/> identifica o passo
/// (<c>entity.projection.read</c>, <c>entity.projection.stale_alert</c>, <c>kafka.apply</c>,
/// <c>kafka.record_failure</c>, <c>kafka.consume</c>, <c>kafka.commit</c>, <c>kafka.resume</c>,
/// <c>kafka.dispatch</c> ou <c>outbox.dispatch</c>).
/// </summary>
public sealed record ScopedDataSyncFailure(
    ScopedDataSyncFailureKind Kind,
    string Scope,
    string? EntityId,
    string Operation,
    Exception Exception);
