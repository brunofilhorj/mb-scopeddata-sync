using MB.ScopedDataSync.Abstractions.Instrumentation;
using MB.ScopedDataSync.Abstractions.Reading;
using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using MB.ScopedDataSync.Caching.Redis.DependencyInjection;
using MB.ScopedDataSync.Consumer.DependencyInjection;
using MB.ScopedDataSync.Observability.DependencyInjection;
using MB.ScopedDataSync.Observability.Metrics;
using MB.ScopedDataSync.Storage.Postgres.DependencyInjection;
using MB.ScopedDataSync.Tests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MB.ScopedDataSync.Tests.Builder;

public class FluentBuilderTests
{
    [Fact]
    public void InMemory_or_redis_can_be_combined_with_optional_postgres()
    {
        var inMemory = new ServiceCollection();
        inMemory.AddScopedDataSync<PositionSnapshot>(builder => builder.UseInMemoryCache());

        var redis = new ServiceCollection();
        redis.AddScopedDataSync<PositionSnapshot>(builder =>
            builder.UseRedisCache(options => options.ConnectionString = "localhost:6379"));

        var inMemoryAndPostgres = new ServiceCollection();
        inMemoryAndPostgres.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.UseInMemoryCache();
            builder.UsePostgresStore(options => options.ConnectionString = "Host=127.0.0.1;Username=mb;Password=mb;Database=entity");
        });

        var redisAndPostgres = new ServiceCollection();
        redisAndPostgres.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.UseRedisCache(options => options.ConnectionString = "localhost:6379");
            builder.UsePostgresStore(options => options.ConnectionString = "Host=127.0.0.1;Username=mb;Password=mb;Database=entity");
        });

        var postgresOnly = new ServiceCollection();
        postgresOnly.AddScopedDataSync<PositionSnapshot>(builder =>
            builder.UsePostgresStore(options =>
            {
                options.ConnectionString = "Host=127.0.0.1;Username=mb;Password=mb;Database=entity";
                options.EnsureSchemaOnStartup = false;
            }));

        Assert.Contains(inMemory, descriptor => descriptor.ServiceType == typeof(IScopeReader<PositionSnapshot>));
        Assert.DoesNotContain(inMemory, descriptor => descriptor.ServiceType == typeof(IHostedService));
        Assert.Contains(redis, descriptor => descriptor.ServiceType == typeof(IScopeReader<PositionSnapshot>));
        Assert.Contains(inMemoryAndPostgres, descriptor => descriptor.ServiceType == typeof(IHostedService));
        Assert.Contains(redisAndPostgres, descriptor => descriptor.ServiceType == typeof(IHostedService));
        Assert.Contains(postgresOnly, descriptor => descriptor.ServiceType == typeof(IScopeReader<PositionSnapshot>));
        Assert.DoesNotContain(postgresOnly, descriptor => descriptor.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public void Builder_rejects_two_cache_providers_and_an_empty_composition()
    {
        var memoryThenRedis = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddScopedDataSync<PositionSnapshot>(builder =>
            {
                builder.UseInMemoryCache();
                builder.UseRedisCache(options => options.ConnectionString = "localhost:6379");
            }));
        var redisThenMemory = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddScopedDataSync<PositionSnapshot>(builder =>
            {
                builder.UseRedisCache(options => options.ConnectionString = "localhost:6379");
                builder.UseInMemoryCache();
            }));
        var empty = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddScopedDataSync<PositionSnapshot>(_ => { }));
        var duplicatedDatabase = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddScopedDataSync<PositionSnapshot>(builder =>
            {
                builder.UsePostgresStore(options => options.ConnectionString = "Host=127.0.0.1;Username=mb;Password=mb;Database=entity");
                builder.UsePostgresStore(options => options.ConnectionString = "Host=127.0.0.1;Username=mb;Password=mb;Database=entity");
            }));

        Assert.Contains("not both", memoryThenRedis.Message, StringComparison.Ordinal);
        Assert.Contains("not both", redisThenMemory.Message, StringComparison.Ordinal);
        Assert.Contains("UseInMemoryCache", empty.Message, StringComparison.Ordinal);
        Assert.Contains("already configured", duplicatedDatabase.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Same_payload_cannot_be_registered_twice_and_distinct_payloads_can()
    {
        var services = new ServiceCollection();
        services.AddScopedDataSync<PositionSnapshot>(builder => builder.UseInMemoryCache());
        services.AddScopedDataSync<AccountProfile>(builder => builder.UseInMemoryCache());

        Assert.Throws<InvalidOperationException>(() =>
            services.AddScopedDataSync<PositionSnapshot>(builder => builder.UseInMemoryCache()));

        using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<IScopeReader<PositionSnapshot>>());
        Assert.NotNull(provider.GetRequiredService<IScopeReader<AccountProfile>>());
    }

    [Fact]
    public void Observability_replaces_the_noop_instrumentation()
    {
        var services = new ServiceCollection();
        services.AddScopedDataSync<PositionSnapshot>(builder => builder.UseInMemoryCache());
        services.AddScopedDataSyncObservability();

        using var provider = services.BuildServiceProvider();
        var instrumentation = provider.GetRequiredService<IScopedDataSyncInstrumentation>();
        instrumentation.EventApplied("positions");
        instrumentation.EventDiscarded("positions");
        instrumentation.SyncFailureRecorded("positions");
        instrumentation.StaleAlertEmitted("positions");
        instrumentation.ProjectionRead("positions", DataFreshnessStatus.Fresh, 12);
        using var lease = instrumentation.Track("entity.projection.apply", "positions");
        lease.SetOutcome("applied");

        Assert.IsType<MetricsScopedDataSyncInstrumentation>(instrumentation);
    }
}
