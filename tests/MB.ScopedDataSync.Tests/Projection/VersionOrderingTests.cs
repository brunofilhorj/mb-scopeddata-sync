using MB.ScopedDataSync.Abstractions.Alerts;
using MB.ScopedDataSync.Abstractions.Errors;
using MB.ScopedDataSync.Abstractions.Instrumentation;
using MB.ScopedDataSync.Abstractions.Projection;
using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using MB.ScopedDataSync.Caching.InMemory.Store;
using MB.ScopedDataSync.Consumer.Projection;
using MB.ScopedDataSync.Consumer.Staleness;
using MB.ScopedDataSync.Tests.Support;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging.Abstractions;

namespace MB.ScopedDataSync.Tests.Projection;

public class VersionOrderingTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 25, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task InMemory_store_keeps_the_highest_version_under_concurrency()
    {
        using var cache = new MemoryCache(new MemoryCacheOptions());
        var store = new InMemoryProjectionStore<PositionSnapshot>(
            cache,
            "positions",
            new InMemoryCacheOptions());

        await Parallel.ForEachAsync(Enumerable.Range(1, 50), async (version, cancellationToken) =>
        {
            await store.TryApplyAsync(
                "inv-1",
                version,
                new PositionSnapshot("PETR4", version),
                Now,
                cancellationToken);
        });

        var current = await store.GetAsync("inv-1", CancellationToken.None);
        var duplicate = await store.TryApplyAsync(
            "inv-1",
            50,
            new PositionSnapshot("PETR4", 1),
            Now,
            CancellationToken.None);
        var older = await store.TryApplyAsync(
            "inv-1",
            49,
            new PositionSnapshot("VALE3", 1),
            Now,
            CancellationToken.None);

        Assert.NotNull(current);
        Assert.Equal(50, current.Version);
        Assert.Equal(50, current.Payload!.Quantity);
        Assert.False(duplicate.Applied);
        Assert.Equal(50, duplicate.CurrentVersion);
        Assert.False(older.Applied);
        Assert.Equal(new PositionSnapshot("PETR4", 50), (await store.GetAsync("inv-1", CancellationToken.None))!.Payload);
    }

    [Fact]
    public async Task InMemory_watermark_rejects_older_versions_after_payload_eviction()
    {
        using var cache = new MemoryCache(new MemoryCacheOptions());
        var store = new InMemoryProjectionStore<PositionSnapshot>(
            cache,
            "positions",
            new InMemoryCacheOptions { PayloadTtl = TimeSpan.FromMinutes(5) });

        var applied = await store.TryApplyAsync("inv-1", 2, new PositionSnapshot("PETR4", 2), Now, CancellationToken.None);
        cache.Remove(InMemoryProjectionStore<PositionSnapshot>.BuildCacheKey("positions", "inv-1"));
        var rejected = await store.TryApplyAsync("inv-1", 1, new PositionSnapshot("PETR4", 1), Now, CancellationToken.None);
        var expired = await store.GetAsync("inv-1", CancellationToken.None);

        Assert.True(applied.Applied);
        Assert.False(rejected.Applied);
        Assert.Equal(2, rejected.CurrentVersion);
        Assert.NotNull(expired);
        Assert.False(expired.HasPayload);
        Assert.Equal(2, expired.Version);
    }

    [Fact]
    public async Task InMemory_failure_does_not_regress_version_or_payload()
    {
        using var cache = new MemoryCache(new MemoryCacheOptions());
        var store = new InMemoryProjectionStore<PositionSnapshot>(
            cache,
            "positions",
            new InMemoryCacheOptions());
        await store.TryApplyAsync("inv-1", 3, new PositionSnapshot("PETR4", 8), Now, CancellationToken.None);

        await store.RecordFailureAsync("inv-1", Now, "broker timeout", CancellationToken.None);
        var rejected = await store.TryApplyAsync("inv-1", 2, new PositionSnapshot("PETR4", 1), Now, CancellationToken.None);
        var current = await store.GetAsync("inv-1", CancellationToken.None);

        Assert.False(rejected.Applied);
        Assert.NotNull(current);
        Assert.Equal(3, current.Version);
        Assert.Equal(new PositionSnapshot("PETR4", 8), current.Payload);
        Assert.Equal(1, current.ConsecutiveFailures);
    }

    [Fact]
    public async Task Durable_rejection_does_not_advance_the_cache()
    {
        var durable = new FakeProjectionStore<PositionSnapshot>();
        var cache = new FakeProjectionStore<PositionSnapshot>();
        await durable.TryApplyAsync("inv-1", 2, new PositionSnapshot("PETR4", 2), Now, CancellationToken.None);
        durable.AppliedAttempts.Clear();
        var coordinator = Coordinator(cache, durable);

        var discarded = await coordinator.ApplyAsync(Envelope(1), CancellationToken.None);
        Assert.Empty(cache.AppliedAttempts);
        var applied = await coordinator.ApplyAsync(Envelope(3, quantity: 3), CancellationToken.None);
        Assert.Equal([3L], cache.AppliedAttempts);
        cache.AppliedAttempts.Clear();
        var duplicate = await coordinator.ApplyAsync(Envelope(3, quantity: 9), CancellationToken.None);

        Assert.Equal(ProjectionApplyOutcome.DiscardedStaleOrDuplicate, discarded.Outcome);
        Assert.Equal(2, discarded.CurrentVersion);
        Assert.Empty(cache.AppliedAttempts);
        Assert.Equal(ProjectionApplyOutcome.Applied, applied.Outcome);
        Assert.Equal(3, applied.CurrentVersion);
        Assert.Equal(ProjectionApplyOutcome.DiscardedStaleOrDuplicate, duplicate.Outcome);
        Assert.Equal(new PositionSnapshot("PETR4", 3), (await cache.GetAsync("inv-1", CancellationToken.None))!.Payload);
    }

    [Fact]
    public async Task Read_uses_the_cache_hit_and_backfills_it_from_postgres_on_miss()
    {
        var durable = new FakeProjectionStore<PositionSnapshot>();
        var cache = new FakeProjectionStore<PositionSnapshot>();
        await cache.TryApplyAsync("inv-1", 4, new PositionSnapshot("PETR4", 4), Now, CancellationToken.None);
        var coordinator = Coordinator(cache, durable);

        var hit = await coordinator.ReadAsync("inv-1", CancellationToken.None);

        Assert.Equal(0, durable.GetCalls);
        Assert.Equal(new PositionSnapshot("PETR4", 4), hit.Data);

        var coldCache = new FakeProjectionStore<PositionSnapshot>();
        await durable.TryApplyAsync("inv-2", 6, new PositionSnapshot("VALE3", 6), Now, CancellationToken.None);
        var coldCoordinator = Coordinator(coldCache, durable);
        var miss = await coldCoordinator.ReadAsync("inv-2", CancellationToken.None);

        Assert.Equal(new PositionSnapshot("VALE3", 6), miss.Data);
        Assert.Equal(6, (await coldCache.GetAsync("inv-2", CancellationToken.None))!.Version);
    }

    private static ProjectionCoordinator<PositionSnapshot> Coordinator(
        FakeProjectionStore<PositionSnapshot> cache,
        FakeProjectionStore<PositionSnapshot> durable)
        => new(
            "positions",
            new StalenessOptions(),
            cache,
            durable,
            new InMemoryAlertCooldownGate(),
            NullStaleAlertSink.Instance,
            NullScopedDataSyncErrorHandler.Instance,
            NoOpScopedDataSyncInstrumentation.Instance,
            TimeProvider.System,
            NullLogger.Instance);

    private static ScopedEventEnvelope<PositionSnapshot> Envelope(long version, int quantity = 1)
        => new("inv-1", "positions", version, new PositionSnapshot("PETR4", quantity), Now);
}
