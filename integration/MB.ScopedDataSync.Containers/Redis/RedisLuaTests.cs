using MB.ScopedDataSync.Caching.Redis.DependencyInjection;
using MB.ScopedDataSync.Caching.Redis.Store;
using MB.ScopedDataSync.Containers.Docker;
using MB.ScopedDataSync.Containers.Support;
using StackExchange.Redis;

namespace MB.ScopedDataSync.Containers.Redis;

public class RedisLuaTests(RedisContainerFixture redis) : IClassFixture<RedisContainerFixture>
{
    private static readonly DateTimeOffset Now = new(2026, 11, 25, 12, 0, 0, TimeSpan.Zero);

    [DockerFact]
    public async Task Lua_apply_keeps_the_highest_version_and_rejects_the_same_version()
    {
        Assert.False(string.IsNullOrWhiteSpace(redis.ConnectionString));
        var options = new RedisCacheOptions
        {
            ConnectionString = redis.ConnectionString,
            ServiceName = "positions-api",
            Database = 0
        };
        await using var multiplexer = await ConnectionMultiplexer.ConnectAsync(redis.ConnectionString);
        var store = new RedisProjectionStore<PositionSnapshot>(multiplexer, options, "positions");

        await Parallel.ForEachAsync(Enumerable.Range(1, 40), async (version, cancellationToken) =>
        {
            await store.TryApplyAsync(
                "inv-1",
                version,
                new PositionSnapshot("PETR4", version),
                Now,
                cancellationToken);
        });

        var duplicate = await store.TryApplyAsync(
            "inv-1",
            40,
            new PositionSnapshot("VALE3", 1),
            Now,
            CancellationToken.None);
        var current = await store.GetAsync("inv-1", CancellationToken.None);

        Assert.False(duplicate.Applied);
        Assert.NotNull(current);
        Assert.Equal(40, current.Version);
        Assert.Equal(new PositionSnapshot("PETR4", 40), current.Payload);
    }
}
