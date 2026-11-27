using System.Text.Json;
using MB.ScopedDataSync.InMemoryExample.Hosting;
using MB.ScopedDataSync.InMemoryExample.Positions;
using MB.ScopedDataSync.Producer.Publishing;

namespace MB.ScopedDataSync.InMemoryExample.Tests;

public class ProducerConsumerExampleTests
{
    [Fact]
    public async Task Published_positions_are_read_by_the_domain_with_the_same_version()
    {
        using var example = new PositionsSyncExample();
        var portfolio = new PortfolioService(example.Reader);
        var published = await example.Publisher.PublishAsync(
            new PublishEntityEventRequest<IReadOnlyList<PositionSnapshot>>(
                "inv-1",
                "positions",
                "entity.positions",
                [new PositionSnapshot("PETR4", 10)]));

        await example.DeliverNewMessagesAsync();
        var read = await example.Reader.ReadAsync("inv-1");
        var positions = await portfolio.GetAsync("inv-1", CancellationToken.None);
        var message = Assert.Single(example.Messages);

        Assert.Equal(1, published.Version);
        Assert.Equal("inv-1", message.Key);
        Assert.Equal(1, read.Freshness.Version);
        Assert.Equal(new PositionSnapshot("PETR4", 10), Assert.Single(positions));
    }

    [Fact]
    public async Task Empty_collection_is_delivered_as_an_empty_array()
    {
        using var example = new PositionsSyncExample();
        var portfolio = new PortfolioService(example.Reader);
        IReadOnlyList<PositionSnapshot>? missing = null;
        await example.Publisher.PublishAsync(
            new PublishEntityEventRequest<IReadOnlyList<PositionSnapshot>>(
                "inv-2",
                "positions",
                "entity.positions",
                missing));

        await example.DeliverNewMessagesAsync();
        var positions = await portfolio.GetAsync("inv-2", CancellationToken.None);
        var message = Assert.Single(example.Messages);
        using var document = JsonDocument.Parse(message.Value);

        Assert.Equal(JsonValueKind.Array, document.RootElement.GetProperty("payload").ValueKind);
        Assert.Equal(0, document.RootElement.GetProperty("payload").GetArrayLength());
        Assert.DoesNotContain("\"payload\":null", message.Value, StringComparison.Ordinal);
        Assert.Empty(positions);
    }

    [Fact]
    public async Task Redelivery_and_an_older_version_do_not_replace_the_snapshot()
    {
        using var example = new PositionsSyncExample();
        var portfolio = new PortfolioService(example.Reader);
        await example.Publisher.PublishAsync(Request("inv-3", 4, at: 0));
        await example.DeliverNewMessagesAsync();
        var first = Assert.Single(example.Messages);

        await example.Publisher.PublishAsync(Request("inv-3", 9, at: 1));
        await example.DeliverNewMessagesAsync();
        await example.RedeliverAsync(first);
        await example.RedeliverAsync(example.Messages[1]);

        var read = await example.Reader.ReadAsync("inv-3");
        var positions = await portfolio.GetAsync("inv-3", CancellationToken.None);

        Assert.Equal(2, read.Freshness.Version);
        Assert.Equal(new PositionSnapshot("PETR4", 9), Assert.Single(positions));
    }

    private static PublishEntityEventRequest<IReadOnlyList<PositionSnapshot>> Request(string entityId, int quantity, int at)
        => new(
            entityId,
            "positions",
            "entity.positions",
            [new PositionSnapshot("PETR4", quantity)],
            new DateTimeOffset(2026, 11, 25, 12, at, 0, TimeSpan.Zero));
}
