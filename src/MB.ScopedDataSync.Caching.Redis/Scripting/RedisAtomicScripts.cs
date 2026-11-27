namespace MB.ScopedDataSync.Caching.Redis.Scripting;

/// <summary>
/// Scripts executados como um único comando no slot de <c>{entityId}</c>.
/// <see cref="TryApply"/> rejeita quando a versão armazenada é maior ou igual à recebida
/// e só então avança a watermark e o payload. A watermark (KEYS[1]) não recebe TTL.
/// <see cref="RecordFailure"/> incrementa falhas e não altera <c>version</c> quando a hash já existe.
/// </summary>
public static class RedisAtomicScripts
{
    public const string TryApply = """
        local incoming = tonumber(ARGV[1])
        local current = redis.call('HGET', KEYS[1], 'version')
        if current and tonumber(current) >= incoming then
          return {0, tonumber(current)}
        end
        redis.call('HSET', KEYS[1],
          'version', ARGV[1],
          'updatedAt', ARGV[3],
          'consecutiveFailures', '0')
        redis.call('HDEL', KEYS[1], 'lastFailureAt', 'lastFailureReason')
        redis.call('SET', KEYS[2], ARGV[2])
        local ttl = tonumber(ARGV[4])
        if ttl > 0 then
          redis.call('PEXPIRE', KEYS[2], ttl)
        else
          redis.call('PERSIST', KEYS[2])
        end
        return {1, incoming}
        """;

    public const string RecordFailure = """
        local failedAt = ARGV[1]
        local reason = ARGV[2]
        if redis.call('EXISTS', KEYS[1]) == 0 then
          redis.call('HSET', KEYS[1],
            'version', '0',
            'consecutiveFailures', '1',
            'lastFailureAt', failedAt,
            'lastFailureReason', reason)
        else
          redis.call('HINCRBY', KEYS[1], 'consecutiveFailures', 1)
          redis.call('HSET', KEYS[1], 'lastFailureAt', failedAt, 'lastFailureReason', reason)
        end
        return 1
        """;

    public const string Read = """
        if redis.call('EXISTS', KEYS[1]) == 0 and redis.call('EXISTS', KEYS[2]) == 0 then
          return nil
        end
        local version = redis.call('HGET', KEYS[1], 'version')
        local updatedAt = redis.call('HGET', KEYS[1], 'updatedAt')
        local failures = redis.call('HGET', KEYS[1], 'consecutiveFailures')
        local lastFailureAt = redis.call('HGET', KEYS[1], 'lastFailureAt')
        local lastFailureReason = redis.call('HGET', KEYS[1], 'lastFailureReason')
        local payload = redis.call('GET', KEYS[2])
        return {version, updatedAt, failures, lastFailureAt, lastFailureReason, payload}
        """;
}
