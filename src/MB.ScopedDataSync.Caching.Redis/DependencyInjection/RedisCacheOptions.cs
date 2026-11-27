using StackExchange.Redis;

namespace MB.ScopedDataSync.Caching.Redis.DependencyInjection;

public sealed class RedisCacheOptions
{
    public string? ConnectionString { get; set; }

    public IConnectionMultiplexer? Multiplexer { get; set; }

    public string ServiceName { get; set; } = "entity-sync";

    public int Database { get; set; } = -1;

    /// <summary>
    /// TTL só da chave de payload. A hash de metadados, onde fica <c>Version</c>, não expira.
    /// </summary>
    public TimeSpan? PayloadTtl { get; set; }
}
