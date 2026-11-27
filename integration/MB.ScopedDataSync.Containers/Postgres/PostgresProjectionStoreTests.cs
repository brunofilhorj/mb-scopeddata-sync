using MB.ScopedDataSync.Abstractions.Projection;
using MB.ScopedDataSync.Abstractions.Reading;
using MB.ScopedDataSync.Consumer.DependencyInjection;
using MB.ScopedDataSync.Containers.Docker;
using MB.ScopedDataSync.Containers.Support;
using MB.ScopedDataSync.Storage.Postgres.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Containers.Postgres;

[Collection(PostgresContainerFixture.CollectionName)]
public class PostgresProjectionStoreTests(PostgresContainerFixture postgres)
{
    private static readonly DateTimeOffset OccurredAt = new(2026, 11, 25, 12, 0, 0, TimeSpan.Zero);

    [DockerFact]
    public async Task Conditional_upsert_keeps_the_higher_version_when_sync_fails()
    {
        Assert.False(string.IsNullOrWhiteSpace(postgres.ConnectionString));
        var services = new ServiceCollection();
        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.UsePostgresStore(options => options.ConnectionString = postgres.ConnectionString);
        });

        await using var provider = services.BuildServiceProvider();
        await HostedServiceRunner.StartAsync(provider, CancellationToken.None);
        try
        {
            var updater = provider.GetRequiredEventConsumerUpdater<PositionSnapshot>();
            var reader = provider.GetRequiredService<IScopeReader<PositionSnapshot>>();
            var applied = await updater.ApplyEventAsync(Envelope(2, 8), CancellationToken.None);
            var older = await updater.ApplyEventAsync(Envelope(1, 1), CancellationToken.None);
            var duplicate = await updater.ApplyEventAsync(Envelope(2, 9), CancellationToken.None);
            await updater.RecordSyncFailureAsync(
                new SyncFailure("inv-store", "Falha ao desserializar o lote.", OccurredAt.AddMinutes(5)),
                CancellationToken.None);
            var read = await reader.ReadAsync("inv-store", CancellationToken.None);

            Assert.Equal(ProjectionApplyOutcome.Applied, applied.Outcome);
            Assert.Equal(ProjectionApplyOutcome.DiscardedStaleOrDuplicate, older.Outcome);
            Assert.Equal(ProjectionApplyOutcome.DiscardedStaleOrDuplicate, duplicate.Outcome);
            Assert.Equal(2, read.Freshness.Version);
            Assert.Equal(new PositionSnapshot("PETR4", 8), read.Data);
            Assert.Equal(1, read.Freshness.ConsecutiveFailures);
        }
        finally
        {
            await HostedServiceRunner.StopAsync(provider, CancellationToken.None);
        }
    }

    private static ScopedEventEnvelope<PositionSnapshot> Envelope(long version, int quantity)
        => new("inv-store", "positions", version, new PositionSnapshot("PETR4", quantity), OccurredAt);
}
