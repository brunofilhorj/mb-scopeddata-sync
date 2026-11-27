using System.Text.Json;
using MB.ScopedDataSync.Abstractions.Errors;
using MB.ScopedDataSync.Producer.DependencyInjection;
using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Postgres;
using MB.ScopedDataSync.Producer.Publishing;
using MB.ScopedDataSync.Tests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

namespace MB.ScopedDataSync.Tests.Producer;

public class ProducerOutboxTests
{
    [Fact]
    public async Task Pipeline_allocates_a_version_and_inserts_the_outbox_with_the_entity_key()
    {
        var transaction = new RecordingOutboxTransaction();
        var first = await OutboxTransactionPipeline.ExecuteAsync(transaction, Draft("inv-1", "positions", "[]"), CancellationToken.None);
        var second = await OutboxTransactionPipeline.ExecuteAsync(transaction, Draft("inv-1", "positions", "{\"symbol\":\"PETR4\"}"), CancellationToken.None);

        Assert.Equal(["increment", "insert", "increment", "insert"], transaction.Operations);
        Assert.Equal(1, first.Version);
        Assert.Equal(2, second.Version);
        Assert.Equal(first.EntityId, first.KafkaKey);
        Assert.Equal("inv-1", second.KafkaKey);
    }

    [Fact]
    public async Task Publisher_assigns_monotonic_versions_per_scope_and_serializes_empty_collections()
    {
        var outbox = new InMemoryEventOutbox();
        var publisher = new ScopedEventPublisher(outbox, TimeProvider.System);

        var removed = await publisher.PublishAsync(
            new PublishEntityEventRequest<IReadOnlyList<PositionSnapshot>>(
                "inv-1",
                "positions",
                "entity.positions",
                Array.Empty<PositionSnapshot>()));
        IReadOnlyList<PositionSnapshot>? missing = null;
        var missingList = await publisher.PublishAsync(
            new PublishEntityEventRequest<IReadOnlyList<PositionSnapshot>>(
                "inv-1",
                "positions",
                "entity.positions",
                missing));
        await publisher.PublishAsync(
            new PublishEntityEventRequest<PositionSnapshot>("inv-1", "profile", "entity.profile", new PositionSnapshot("PETR4", 1)));

        await Parallel.ForEachAsync(Enumerable.Range(0, 30), async (_, cancellationToken) =>
        {
            await publisher.PublishAsync(
                new PublishEntityEventRequest<PositionSnapshot>(
                    "inv-9",
                    "positions",
                    "entity.positions",
                    new PositionSnapshot("PETR4", 1)),
                cancellationToken);
        });

        var message = OutboxKafkaMessage.Create(removed);
        using var document = JsonDocument.Parse(message.Value);
        var concurrent = outbox.Events.Where(item => item.EntityId == "inv-9").Select(item => item.Version).Order().ToArray();

        Assert.Equal("[]", removed.PayloadJson);
        Assert.Equal("[]", missingList.PayloadJson);
        Assert.Equal("inv-1", message.Key);
        Assert.Equal(JsonValueKind.Array, document.RootElement.GetProperty("payload").ValueKind);
        Assert.Equal(0, document.RootElement.GetProperty("payload").GetArrayLength());
        Assert.Equal(1, removed.Version);
        Assert.Equal(2, missingList.Version);
        Assert.Equal(1, outbox.Events.Single(item => item.Scope == "profile").Version);
        Assert.Equal(Enumerable.Range(1, 30).Select(version => (long)version).ToArray(), concurrent);
        Assert.All(outbox.Events, item => Assert.Equal(item.EntityId, item.KafkaKey));
        Assert.Throws<ArgumentNullException>(() => PayloadNormalizer.ToJson<PositionSnapshot>(null));
    }

    [Fact]
    public async Task Relay_loop_retries_after_a_dispatch_failure_and_keeps_the_handler_contained()
    {
        using var cancellation = new CancellationTokenSource();
        var handler = new RecordingErrorHandler { Throw = new InvalidOperationException("handler down") };
        var calls = 0;

        await new OutboxRelayLoop().RunAsync(
            _ =>
            {
                calls++;
                if (calls == 1)
                {
                    return Task.FromException<int>(new IOException("broker down"));
                }

                cancellation.Cancel();
                return Task.FromResult(0);
            },
            TimeSpan.Zero,
            handler,
            NullLogger.Instance,
            TimeProvider.System,
            cancellation.Token);

        var failure = Assert.Single(handler.Failures);
        Assert.Equal(ScopedDataSyncFailureKind.OutboxRelayFaulted, failure.Kind);
        Assert.Equal("outbox.dispatch", failure.Operation);
        Assert.Equal(2, calls);
    }

    [Fact]
    public void Relay_registration_rejects_a_second_loop_and_a_non_positive_batch()
    {
        var services = new ServiceCollection();
        services.AddScopedDataSyncOutboxRelay();
        var duplicate = Assert.Throws<InvalidOperationException>(() => services.AddScopedDataSyncOutboxRelay());
        var batch = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new ServiceCollection().AddScopedDataSyncOutboxRelay(options => options.BatchSize = 0));

        Assert.Contains("already registered", duplicate.Message, StringComparison.Ordinal);
        Assert.Equal(0, batch.ActualValue);
    }

    [Fact]
    public void Postgres_outbox_sql_increments_version_and_stores_the_kafka_key_together()
    {
        Assert.Contains("version = entity_scope_versions.version + 1", PostgresOutboxSql.IncrementVersion, StringComparison.Ordinal);
        Assert.Contains("RETURNING version", PostgresOutboxSql.IncrementVersion, StringComparison.Ordinal);
        Assert.Contains("kafka_key", PostgresOutboxSql.InsertOutbox, StringComparison.Ordinal);
        Assert.Contains("FOR UPDATE SKIP LOCKED", PostgresOutboxSql.LeaseUnpublished, StringComparison.Ordinal);
        Assert.Contains("ux_outbox_events_scope_version", PostgresOutboxSql.EnsureSchema, StringComparison.Ordinal);
    }

    private static NewOutboxEvent Draft(string entityId, string scope, string payload)
        => new(Guid.CreateVersion7(), entityId, scope, "entity." + scope, payload, DateTimeOffset.UnixEpoch);
}
