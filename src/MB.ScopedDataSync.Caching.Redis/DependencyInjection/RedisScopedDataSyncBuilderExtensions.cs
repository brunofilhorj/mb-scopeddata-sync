namespace MB.ScopedDataSync.Caching.Redis.DependencyInjection;

public static class RedisScopedDataSyncBuilderExtensions
{
    public static ScopedDataSyncBuilder<TPayload> UseRedisCache<TPayload>(
        this ScopedDataSyncBuilder<TPayload> builder,
        Action<RedisCacheOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new RedisCacheOptions();
        configure(options);
        SyncNameRules.ThrowIfInvalidServiceName(options.ServiceName);
        if (options.Multiplexer is null && string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException("Redis cache requires a connection string or an IConnectionMultiplexer.");
        }

        if (options.PayloadTtl is { } ttl && ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(configure), "PayloadTtl must be greater than zero when set.");
        }

        return builder.UseCacheProvider(new RedisCacheProviderRegistration<TPayload>(options));
    }
}
