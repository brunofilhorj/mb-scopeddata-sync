using System.Collections.Concurrent;
using MB.ScopedDataSync.Caching.Redis.DependencyInjection;
using StackExchange.Redis;

namespace MB.ScopedDataSync.Caching.Redis.Store;

internal sealed class RedisMultiplexerRegistry : IDisposable
{
    private readonly ConcurrentDictionary<string, IConnectionMultiplexer> _owned = new(StringComparer.Ordinal);

    public IConnectionMultiplexer Resolve(RedisCacheOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Multiplexer is not null)
        {
            return options.Multiplexer;
        }

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException("Redis cache requires a connection string or an IConnectionMultiplexer.");
        }

        return _owned.GetOrAdd(options.ConnectionString, static connectionString => ConnectionMultiplexer.Connect(connectionString));
    }

    public void Dispose()
    {
        foreach (var multiplexer in _owned.Values)
        {
            multiplexer.Dispose();
        }

        _owned.Clear();
    }
}
