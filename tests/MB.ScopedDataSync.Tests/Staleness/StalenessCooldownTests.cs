using MB.ScopedDataSync.Abstractions.Alerts;
using MB.ScopedDataSync.Abstractions.Projection;
using MB.ScopedDataSync.Abstractions.Reading;
using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using MB.ScopedDataSync.Consumer.DependencyInjection;
using MB.ScopedDataSync.Consumer.Staleness;
using MB.ScopedDataSync.Tests.Support;
using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Tests.Staleness;

public class StalenessCooldownTests
{
    [Fact]
    public async Task Stale_read_alerts_once_inside_the_cooldown_window()
    {
        var time = new FixedTimeProvider(new DateTimeOffset(2026, 11, 25, 12, 0, 0, TimeSpan.Zero));
        var sink = new RecordingAlertSink();
        var services = new ServiceCollection();
        services.AddSingleton<IStaleAlertSink>(sink);
        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.WithStaleness(options =>
            {
                options.StaleAfter = TimeSpan.FromMinutes(5);
                options.AlertCooldown = TimeSpan.FromMinutes(10);
            });
            builder.UseTimeProvider(time);
            builder.UseInMemoryCache();
        });

        using var provider = services.BuildServiceProvider();
        var updater = provider.GetRequiredEventConsumerUpdater<PositionSnapshot>();
        var reader = provider.GetRequiredService<IScopeReader<PositionSnapshot>>();
        await updater.ApplyEventAsync(
            new ScopedEventEnvelope<PositionSnapshot>("inv-1", "positions", 1, new PositionSnapshot("PETR4", 10), time.UtcNow),
            CancellationToken.None);

        var fresh = await reader.ReadAsync("inv-1");
        Assert.Equal(DataFreshnessStatus.Fresh, fresh.Freshness.Status);
        Assert.Empty(sink.Alerts);

        time.UtcNow = time.UtcNow.AddMinutes(5);
        var stale = await reader.ReadAsync("inv-1");
        Assert.Equal(DataFreshnessStatus.Stale, stale.Freshness.Status);
        Assert.False(stale.Freshness.AlertSuppressedByCooldown);
        Assert.Equal(new PositionSnapshot("PETR4", 10), stale.Data);
        Assert.Single(sink.Alerts);

        var suppressed = await reader.ReadAsync("inv-1");
        Assert.True(suppressed.Freshness.AlertSuppressedByCooldown);
        Assert.Single(sink.Alerts);

        time.UtcNow = time.UtcNow.AddMinutes(10);
        var alertedAgain = await reader.ReadAsync("inv-1");
        Assert.False(alertedAgain.Freshness.AlertSuppressedByCooldown);
        Assert.Equal(2, sink.Alerts.Count);
        Assert.All(sink.Alerts, alert => Assert.Equal("inv-1", alert.EntityId));
    }

    [Fact]
    public async Task Sync_failure_inside_the_fresh_window_does_not_raise_a_stale_alert()
    {
        var time = new FixedTimeProvider(new DateTimeOffset(2026, 11, 25, 12, 0, 0, TimeSpan.Zero));
        var sink = new RecordingAlertSink();
        var services = new ServiceCollection();
        services.AddSingleton<IStaleAlertSink>(sink);
        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.WithStaleness(options => options.StaleAfter = TimeSpan.FromMinutes(5));
            builder.UseTimeProvider(time);
            builder.UseInMemoryCache();
        });
        using var provider = services.BuildServiceProvider();
        var updater = provider.GetRequiredEventConsumerUpdater<PositionSnapshot>();
        await updater.ApplyEventAsync(
            new ScopedEventEnvelope<PositionSnapshot>("inv-1", "positions", 4, new PositionSnapshot("PETR4", 4), time.UtcNow),
            CancellationToken.None);
        await updater.RecordSyncFailureAsync(new SyncFailure("inv-1", "timeout", time.UtcNow), CancellationToken.None);

        var impaired = await provider.GetRequiredService<IScopeReader<PositionSnapshot>>().ReadAsync("inv-1");

        Assert.Equal(DataFreshnessStatus.SyncImpaired, impaired.Freshness.Status);
        Assert.Equal(4, impaired.Freshness.Version);
        Assert.Equal(1, impaired.Freshness.ConsecutiveFailures);
        Assert.Equal(new PositionSnapshot("PETR4", 4), impaired.Data);
        Assert.Empty(sink.Alerts);
    }

    [Fact]
    public void Cooldown_gate_admits_a_single_winner_for_the_same_key()
    {
        var gate = new InMemoryAlertCooldownGate();
        var now = DateTimeOffset.UtcNow;
        var wins = 0;

        Parallel.For(0, 32, _ =>
        {
            if (gate.TryEnter("positions:inv-1", TimeSpan.FromMinutes(1), now))
            {
                Interlocked.Increment(ref wins);
            }
        });

        Assert.Equal(1, wins);
        Assert.False(gate.TryEnter("positions:inv-1", TimeSpan.FromMinutes(1), now));
    }
}
