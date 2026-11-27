using MB.ScopedDataSync.Abstractions.Projection;

namespace MB.ScopedDataSync.Tests.Support;

internal sealed class FakeProjectionStore<TPayload> : IProjectionStore<TPayload>
{
    private long _version;
    private TPayload? _payload;
    private bool _hasPayload;
    private DateTimeOffset? _updatedAt;
    private int _failures;

    public int GetCalls { get; private set; }

    public List<long> AppliedAttempts { get; } = [];

    public Task<ProjectionRecord<TPayload>?> GetAsync(string entityId, CancellationToken cancellationToken)
    {
        GetCalls++;
        if (_version == 0 && !_hasPayload && _failures == 0)
        {
            return Task.FromResult<ProjectionRecord<TPayload>?>(null);
        }

        return Task.FromResult<ProjectionRecord<TPayload>?>(
            new ProjectionRecord<TPayload>(_version, _payload, _hasPayload, _updatedAt, _failures, null, null));
    }

    public Task<ProjectionWriteResult> TryApplyAsync(
        string entityId,
        long version,
        TPayload payload,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken)
    {
        AppliedAttempts.Add(version);
        if (_version >= version)
        {
            return Task.FromResult(new ProjectionWriteResult(false, _version));
        }

        _version = version;
        _payload = payload;
        _hasPayload = true;
        _updatedAt = updatedAtUtc;
        _failures = 0;
        return Task.FromResult(new ProjectionWriteResult(true, version));
    }

    public Task RecordFailureAsync(
        string entityId,
        DateTimeOffset failedAtUtc,
        string reason,
        CancellationToken cancellationToken)
    {
        _failures++;
        return Task.CompletedTask;
    }
}
