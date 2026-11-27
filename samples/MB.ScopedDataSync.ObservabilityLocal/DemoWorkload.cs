using MB.ScopedDataSync.Abstractions.Projection;
using MB.ScopedDataSync.Abstractions.Reading;
using MB.ScopedDataSync.Consumer.DependencyInjection;

namespace MB.ScopedDataSync.ObservabilityLocal;

internal sealed class DemoWorkload(
    IServiceProvider services,
    ILogger<DemoWorkload> logger) : BackgroundService
{
    private static readonly PositionSnapshot Payload = new("PETR4", 10);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var updater = services.GetRequiredEventConsumerUpdater<PositionSnapshot>();
        var reader = services.GetRequiredService<IScopeReader<PositionSnapshot>>();
        long freshVersion = 0;
        long impairedVersion = 0;

        await updater.ApplyEventAsync(
            Envelope("inv-stale", 1, DateTimeOffset.UtcNow.AddMinutes(-10)),
            stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            freshVersion++;
            await updater.ApplyEventAsync(Envelope("inv-fresh", freshVersion, DateTimeOffset.UtcNow), stoppingToken);
            if (freshVersion > 1)
            {
                await updater.ApplyEventAsync(Envelope("inv-fresh", 1, DateTimeOffset.UtcNow), stoppingToken);
            }

            impairedVersion++;
            await updater.ApplyEventAsync(Envelope("inv-impaired", impairedVersion, DateTimeOffset.UtcNow), stoppingToken);
            await updater.RecordSyncFailureAsync(
                new SyncFailure("inv-impaired", "upstream timeout", DateTimeOffset.UtcNow),
                stoppingToken);

            var fresh = await reader.ReadAsync("inv-fresh", stoppingToken);
            var stale = await reader.ReadAsync("inv-stale", stoppingToken);
            var impaired = await reader.ReadAsync("inv-impaired", stoppingToken);
            await reader.ReadAsync("inv-missing", stoppingToken);

            logger.LogInformation(
                "positions fresh={Fresh} stale={Stale} impaired={Impaired} staleAlertSuppressed={Suppressed}",
                fresh.Freshness.Status,
                stale.Freshness.Status,
                impaired.Freshness.Status,
                stale.Freshness.AlertSuppressedByCooldown);

            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }

    private static ScopedEventEnvelope<PositionSnapshot> Envelope(string entityId, long version, DateTimeOffset occurredAtUtc)
        => new(entityId, "positions", version, Payload, occurredAtUtc);
}
