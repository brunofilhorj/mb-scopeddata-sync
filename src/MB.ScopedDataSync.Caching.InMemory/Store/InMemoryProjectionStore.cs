using System.Collections.Concurrent;
using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using Microsoft.Extensions.Caching.Memory;

namespace MB.ScopedDataSync.Caching.InMemory.Store;

/// <summary>
/// Cache em processo. A comparação <c>incoming.Version &gt; current.Version</c> e a troca do payload
/// ocorrem sob o mesmo lock da entidade. O TTL vale só para o payload no <see cref="IMemoryCache"/>;
/// a watermark de versão permanece e continua rejeitando eventos atrasados depois da expiração.
/// Falha consecutiva altera contadores sem mover a watermark.
/// </summary>
public sealed class InMemoryProjectionStore<TPayload> : IProjectionStore<TPayload>
{
    private readonly IMemoryCache _cache;
    private readonly string _scope;
    private readonly TimeSpan? _payloadTtl;
    private readonly ConcurrentDictionary<string, VersionSlot> _slots = new(StringComparer.Ordinal);

    public InMemoryProjectionStore(IMemoryCache cache, string scope, InMemoryCacheOptions options)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(options);
        SyncNameRules.ThrowIfInvalidScope(scope);
        if (options.PayloadTtl is { } ttl && ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "PayloadTtl must be greater than zero when set.");
        }

        _cache = cache;
        _scope = scope;
        _payloadTtl = options.PayloadTtl;
    }

    public static string BuildCacheKey(string scope, string entityId) => $"mbsds:{scope}:{entityId}";

    public Task<ProjectionRecord<TPayload>?> GetAsync(string entityId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        var slot = _slots.GetOrAdd(entityId, static _ => new VersionSlot());
        lock (slot.Gate)
        {
            if (slot.Version == 0 && slot.ConsecutiveFailures == 0)
            {
                return Task.FromResult<ProjectionRecord<TPayload>?>(null);
            }

            if (_cache.TryGetValue(BuildCacheKey(_scope, entityId), out CacheEntry? entry)
                && entry is not null
                && entry.Version == slot.Version)
            {
                return Task.FromResult<ProjectionRecord<TPayload>?>(new ProjectionRecord<TPayload>(
                    slot.Version,
                    entry.Payload,
                    HasPayload: true,
                    entry.UpdatedAtUtc,
                    slot.ConsecutiveFailures,
                    slot.LastFailureAtUtc,
                    slot.LastFailureReason));
            }

            return Task.FromResult<ProjectionRecord<TPayload>?>(new ProjectionRecord<TPayload>(
                slot.Version,
                default,
                HasPayload: false,
                UpdatedAtUtc: null,
                slot.ConsecutiveFailures,
                slot.LastFailureAtUtc,
                slot.LastFailureReason));
        }
    }

    public Task<ProjectionWriteResult> TryApplyAsync(
        string entityId,
        long version,
        TPayload payload,
        DateTimeOffset updatedAtUtc,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        ArgumentNullException.ThrowIfNull(payload);
        if (version <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(version));
        }

        var slot = _slots.GetOrAdd(entityId, static _ => new VersionSlot());
        lock (slot.Gate)
        {
            if (slot.Version >= version)
            {
                return Task.FromResult(new ProjectionWriteResult(false, slot.Version));
            }

            slot.Version = version;
            slot.ConsecutiveFailures = 0;
            slot.LastFailureAtUtc = null;
            slot.LastFailureReason = null;

            var entryOptions = new MemoryCacheEntryOptions();
            if (_payloadTtl is { } ttl)
            {
                entryOptions.AbsoluteExpirationRelativeToNow = ttl;
            }

            _cache.Set(
                BuildCacheKey(_scope, entityId),
                new CacheEntry(version, payload, updatedAtUtc.ToUniversalTime()),
                entryOptions);

            return Task.FromResult(new ProjectionWriteResult(true, version));
        }
    }

    public Task RecordFailureAsync(
        string entityId,
        DateTimeOffset failedAtUtc,
        string reason,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);

        var slot = _slots.GetOrAdd(entityId, static _ => new VersionSlot());
        lock (slot.Gate)
        {
            slot.ConsecutiveFailures++;
            slot.LastFailureAtUtc = failedAtUtc.ToUniversalTime();
            slot.LastFailureReason = reason;
        }

        return Task.CompletedTask;
    }

    private sealed class VersionSlot
    {
        public object Gate { get; } = new();

        public long Version { get; set; }

        public int ConsecutiveFailures { get; set; }

        public DateTimeOffset? LastFailureAtUtc { get; set; }

        public string? LastFailureReason { get; set; }
    }

    private sealed record CacheEntry(long Version, TPayload Payload, DateTimeOffset UpdatedAtUtc);
}
