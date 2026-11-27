using System.Text.Json;
using MB.ScopedDataSync.Containers.Docker;
using MB.ScopedDataSync.Containers.Support;
using MB.ScopedDataSync.Producer.DependencyInjection;
using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Postgres;
using MB.ScopedDataSync.Producer.Publishing;
using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Containers.Postgres;

[Collection(PostgresContainerFixture.CollectionName)]
public class PostgresProducerTests(PostgresContainerFixture postgres)
{
    [DockerFact]
    public async Task Outbox_allocates_versions_and_the_relay_publishes_each_row_once()
    {
        Assert.False(string.IsNullOrWhiteSpace(postgres.ConnectionString));
        var kafka = new RecordingOutboxPublisher();
        var services = new ServiceCollection();
        services.AddScopedDataSyncProducer(options => options.ConnectionString = postgres.ConnectionString);
        services.AddSingleton<IOutboxKafkaPublisher>(kafka);
        services.AddScopedDataSyncOutboxRelay();

        await using var provider = services.BuildServiceProvider();
        await HostedServiceRunner.StartAsync(provider, CancellationToken.None);
        try
        {
            var publisher = provider.GetRequiredService<IScopedEventPublisher>();
            var occurredAt = new DateTimeOffset(2026, 11, 25, 12, 0, 0, TimeSpan.Zero);
            var first = await publisher.PublishAsync(
                new PublishEntityEventRequest<IReadOnlyList<PositionSnapshot>>(
                    "inv-1",
                    "positions",
                    "entity.positions",
                    [new PositionSnapshot("PETR4", 4)],
                    occurredAt));
            var second = await publisher.PublishAsync(
                new PublishEntityEventRequest<IReadOnlyList<PositionSnapshot>>(
                    "inv-1",
                    "positions",
                    "entity.positions",
                    Array.Empty<PositionSnapshot>(),
                    occurredAt.AddMinutes(1)));

            var relay = provider.GetRequiredService<PostgresOutboxRelay>();
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (kafka.Messages.Count < 2 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(50);
            }

            var again = await relay.DispatchAsync(10, CancellationToken.None);
            using var document = JsonDocument.Parse(kafka.Messages[1].Value);

            Assert.Equal(1, first.Version);
            Assert.Equal(2, second.Version);
            Assert.Equal("inv-1", first.KafkaKey);
            Assert.Equal("[]", second.PayloadJson);
            Assert.Equal(2, kafka.Messages.Count);
            Assert.Equal(0, again);
            Assert.Equal(["inv-1", "inv-1"], kafka.Messages.Select(message => message.Key).ToArray());
            Assert.Equal(JsonValueKind.Array, document.RootElement.GetProperty("payload").ValueKind);
            Assert.Equal(0, document.RootElement.GetProperty("payload").GetArrayLength());
        }
        finally
        {
            await HostedServiceRunner.StopAsync(provider, CancellationToken.None);
        }
    }
}
