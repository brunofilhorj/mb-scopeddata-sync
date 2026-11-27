using System.Globalization;
using System.Text.Json;
using MB.ScopedDataSync.Caching.Redis.DependencyInjection;
using MB.ScopedDataSync.Caching.Redis.Scripting;
using StackExchange.Redis;

namespace MB.ScopedDataSync.Caching.Redis.Store;

/// <summary>
/// Cache Redis. Cada operação de versão ou falha é um script Lua nas chaves da mesma hash tag.
/// </summary>
public sealed class RedisProjectionStore<TPayload> : IProjectionStore<TPayload>
{
    private readonly IDatabase _database;
    private readonly string _serviceName;
    private readonly string _scope;
    private readonly long _payloadTtlMilliseconds;

    public RedisProjectionStore(IConnectionMultiplexer multiplexer, RedisCacheOptions options, string scope)
    {
        ArgumentNullException.ThrowIfNull(multiplexer);
        ArgumentNullException.ThrowIfNull(options);
        SyncNameRules.ThrowIfInvalidServiceName(options.ServiceName);
        SyncNameRules.ThrowIfInvalidScope(scope);
        if (options.PayloadTtl is { } ttl && ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "PayloadTtl must be greater than zero when set.");
        }

        _database = multiplexer.GetDatabase(options.Database);
        _serviceName = options.ServiceName;
        _scope = scope;
        _payloadTtlMilliseconds = options.PayloadTtl is { } configured
            ? (long)configured.TotalMilliseconds
            : 0;
    }

    public async Task<ProjectionRecord<TPayload>?> GetAsync(string entityId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var keys = Keys(entityId);
        var result = await _database.ScriptEvaluateAsync(
            RedisAtomicScripts.Read,
            [keys.MetaKey, keys.PayloadKey]).ConfigureAwait(false);

        if (result.IsNull)
        {
            return null;
        }

        var values = (RedisResult[])result!;
        var versionText = Text(values[0]);
        var updatedAtText = Text(values[1]);
        var failuresText = Text(values[2]);
        var lastFailureAtText = Text(values[3]);
        var lastFailureReason = Text(values[4]);
        var payloadText = Text(values[5]);
        var version = versionText is null ? 0 : long.Parse(versionText, CultureInfo.InvariantCulture);
        TPayload? payload = default;
        var hasPayload = false;
        if (!string.IsNullOrEmpty(payloadText))
        {
            payload = JsonSerializer.Deserialize<TPayload>(payloadText, ScopedDataSyncJson.Options);
            hasPayload = payload is not null;
        }

        if (version == 0 && !hasPayload && failuresText is null)
        {
            return null;
        }

        return new ProjectionRecord<TPayload>(
            version,
            payload,
            hasPayload,
            ParseTimestamp(updatedAtText),
            failuresText is null ? 0 : int.Parse(failuresText, CultureInfo.InvariantCulture),
            ParseTimestamp(lastFailureAtText),
            lastFailureReason);
    }

    public async Task<ProjectionWriteResult> TryApplyAsync(
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

        var keys = Keys(entityId);
        var json = JsonSerializer.Serialize(payload, ScopedDataSyncJson.Options);
        var result = await _database.ScriptEvaluateAsync(
            RedisAtomicScripts.TryApply,
            [keys.MetaKey, keys.PayloadKey],
            [
                version,
                json,
                updatedAtUtc.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
                _payloadTtlMilliseconds
            ]).ConfigureAwait(false);

        var values = (RedisResult[])result!;
        var applied = (long)values[0] == 1;
        var current = (long)values[1];
        return new ProjectionWriteResult(applied, current);
    }

    public async Task RecordFailureAsync(
        string entityId,
        DateTimeOffset failedAtUtc,
        string reason,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        var keys = Keys(entityId);
        await _database.ScriptEvaluateAsync(
            RedisAtomicScripts.RecordFailure,
            [(RedisKey)keys.MetaKey],
            [
                failedAtUtc.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
                reason
            ]).ConfigureAwait(false);
    }

    private (string MetaKey, string PayloadKey) Keys(string entityId)
        => RedisKeyBuilder.For(_serviceName, _scope, entityId);

    private static string? Text(RedisResult value) => value.IsNull ? null : (string?)value;

    private static DateTimeOffset? ParseTimestamp(string? value)
        => string.IsNullOrEmpty(value)
            ? null
            : DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
}
