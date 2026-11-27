using System.Diagnostics;
using MB.ScopedDataSync.Abstractions.Alerts;
using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using MB.ScopedDataSync.Abstractions.Errors;
using MB.ScopedDataSync.Abstractions.Instrumentation;
using MB.ScopedDataSync.Abstractions.Projection;
using MB.ScopedDataSync.Abstractions.Reading;
using MB.ScopedDataSync.Consumer.DependencyInjection;
using MB.ScopedDataSync.Tests.Support;
using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Tests.Projection;

public class ContainedFailureTests
{
    [Fact]
    public async Task Cache_read_failure_falls_back_to_the_database_and_notifies()
    {
        var database = new FakeProjectionStore<PositionSnapshot>();
        await database.TryApplyAsync(
            "inv-1",
            3,
            new PositionSnapshot("PETR4", 8),
            DateTimeOffset.UtcNow,
            CancellationToken.None);
        var handler = new RecordingErrorHandler { Throw = new InvalidOperationException("handler down") };
        await using var provider = Build(handler, new ThrowingGetStore<PositionSnapshot>(), database);

        var read = await provider.GetRequiredService<IScopeReader<PositionSnapshot>>().ReadAsync("inv-1");

        Assert.Equal(new PositionSnapshot("PETR4", 8), read.Data);
        var failure = Assert.Single(handler.Failures);
        Assert.Equal(ScopedDataSyncFailureKind.CacheDegraded, failure.Kind);
        Assert.Equal("positions", failure.Scope);
        Assert.Equal("inv-1", failure.EntityId);
        Assert.Equal("entity.projection.read", failure.Operation);
        Assert.IsType<IOException>(failure.Exception);
    }

    [Fact]
    public async Task Cache_read_failure_without_a_database_reaches_the_caller()
    {
        var handler = new RecordingErrorHandler();
        await using var provider = Build(handler, new ThrowingGetStore<PositionSnapshot>(), database: null);

        var exception = await Assert.ThrowsAsync<IOException>(() =>
            provider.GetRequiredService<IScopeReader<PositionSnapshot>>().ReadAsync("inv-1"));

        Assert.Equal("cache down", exception.Message);
        Assert.Empty(handler.Failures);
    }

    [Fact]
    public async Task Direct_apply_failure_reaches_the_caller()
    {
        var handler = new RecordingErrorHandler();
        await using var provider = Build(handler, new ThrowingApplyStore<PositionSnapshot>(), database: null);

        var exception = await Assert.ThrowsAsync<IOException>(() =>
            provider.GetRequiredEventConsumerUpdater<PositionSnapshot>().ApplyEventAsync(
                new ScopedEventEnvelope<PositionSnapshot>(
                    "inv-1",
                    "positions",
                    1,
                    new PositionSnapshot("PETR4", 1),
                    DateTimeOffset.UtcNow),
                CancellationToken.None));

        Assert.Equal("store down", exception.Message);
        Assert.Empty(handler.Failures);
    }

    [Fact]
    public async Task Stale_alert_failure_keeps_the_payload_and_notifies()
    {
        var time = new FixedTimeProvider(new DateTimeOffset(2026, 11, 26, 12, 0, 0, TimeSpan.Zero));
        var handler = new RecordingErrorHandler { Throw = new InvalidOperationException("handler down") };
        var services = new ServiceCollection();
        services.AddSingleton<IScopedDataSyncErrorHandler>(handler);
        services.AddSingleton<IStaleAlertSink>(new ThrowingAlertSink());
        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.WithStaleness(options => options.StaleAfter = TimeSpan.FromMinutes(5));
            builder.UseTimeProvider(time);
            builder.UseInMemoryCache();
        });
        await using var provider = services.BuildServiceProvider();
        await provider.GetRequiredEventConsumerUpdater<PositionSnapshot>().ApplyEventAsync(
            new ScopedEventEnvelope<PositionSnapshot>(
                "inv-1",
                "positions",
                2,
                new PositionSnapshot("PETR4", 4),
                time.UtcNow),
            CancellationToken.None);
        time.UtcNow = time.UtcNow.AddMinutes(5);

        var read = await provider.GetRequiredService<IScopeReader<PositionSnapshot>>().ReadAsync("inv-1");

        Assert.Equal(new PositionSnapshot("PETR4", 4), read.Data);
        Assert.Equal(DataFreshnessStatus.Stale, read.Freshness.Status);
        var failure = Assert.Single(handler.Failures);
        Assert.Equal(ScopedDataSyncFailureKind.StaleAlertFailed, failure.Kind);
        Assert.Equal("inv-1", failure.EntityId);
        Assert.Equal("entity.projection.stale_alert", failure.Operation);
    }

    [Fact]
    public void SetError_marks_the_activity()
    {
        using var source = new ActivitySource("MB.ScopedDataSync.Tests");
        using var listener = new ActivityListener
        {
            ShouldListenTo = candidate => candidate.Name == source.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(listener);
        using var activity = source.StartActivity("entity.projection.read");
        var lease = new InstrumentationLease(activity);

        lease.SetError(new IOException("down"));

        Assert.NotNull(activity);
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("error", activity.GetTagItem("entity.outcome"));
    }

    private static ServiceProvider Build(
        RecordingErrorHandler handler,
        IProjectionStore<PositionSnapshot> cache,
        IProjectionStore<PositionSnapshot>? database)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IScopedDataSyncErrorHandler>(handler);
        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.UseCacheProvider(new FixedCache(cache));
            if (database is not null)
            {
                builder.UseDatabaseStore(new FixedDatabase(database));
            }
        });
        return services.BuildServiceProvider();
    }

    private sealed class FixedCache(IProjectionStore<PositionSnapshot> store) : CacheProviderRegistration<PositionSnapshot>
    {
        public override IProjectionStore<PositionSnapshot> CreateStore(
            IServiceProvider serviceProvider,
            ScopedDataSyncRegistrationContext context)
            => store;
    }

    private sealed class FixedDatabase(IProjectionStore<PositionSnapshot> store) : DatabaseStoreRegistration<PositionSnapshot>
    {
        public override IProjectionStore<PositionSnapshot> CreateStore(
            IServiceProvider serviceProvider,
            ScopedDataSyncRegistrationContext context)
            => store;
    }

    private sealed class ThrowingGetStore<TPayload> : IProjectionStore<TPayload>
    {
        public Task<ProjectionRecord<TPayload>?> GetAsync(string entityId, CancellationToken cancellationToken)
            => throw new IOException("cache down");

        public Task<ProjectionWriteResult> TryApplyAsync(
            string entityId,
            long version,
            TPayload payload,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken)
            => Task.FromResult(new ProjectionWriteResult(true, version));

        public Task RecordFailureAsync(
            string entityId,
            DateTimeOffset failedAtUtc,
            string reason,
            CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class ThrowingApplyStore<TPayload> : IProjectionStore<TPayload>
    {
        public Task<ProjectionRecord<TPayload>?> GetAsync(string entityId, CancellationToken cancellationToken)
            => Task.FromResult<ProjectionRecord<TPayload>?>(null);

        public Task<ProjectionWriteResult> TryApplyAsync(
            string entityId,
            long version,
            TPayload payload,
            DateTimeOffset updatedAtUtc,
            CancellationToken cancellationToken)
            => throw new IOException("store down");

        public Task RecordFailureAsync(
            string entityId,
            DateTimeOffset failedAtUtc,
            string reason,
            CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class ThrowingAlertSink : IStaleAlertSink
    {
        public Task PublishAsync(StaleRecordAlert alert, CancellationToken cancellationToken = default)
            => throw new IOException("sink down");
    }
}
