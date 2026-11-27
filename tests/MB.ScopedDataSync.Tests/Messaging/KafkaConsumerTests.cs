using System.Text.Json;
using MB.ScopedDataSync.Abstractions.Errors;
using MB.ScopedDataSync.Abstractions.Projection;
using MB.ScopedDataSync.Abstractions.Reading;
using MB.ScopedDataSync.Abstractions.Serialization;
using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using MB.ScopedDataSync.Consumer.DependencyInjection;
using Confluent.Kafka;
using MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;
using MB.ScopedDataSync.Messaging.Kafka.Dispatch;
using MB.ScopedDataSync.Messaging.Kafka.Hosting;
using MB.ScopedDataSync.Messaging.Kafka.Publishing;
using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Tests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MB.ScopedDataSync.Tests.Messaging;

public class KafkaConsumerTests
{
    [Fact]
    public async Task Topic_registration_is_visible_to_the_host_in_either_order()
    {
        var kafkaFirst = await SubscribeAsync(kafkaBeforeScopes: true);
        var scopesFirst = await SubscribeAsync(kafkaBeforeScopes: false);

        Assert.Equal(["entity.positions", "entity.profile"], kafkaFirst);
        Assert.Equal(["entity.positions", "entity.profile"], scopesFirst);
    }

    [Fact]
    public async Task Host_start_requires_both_the_client_and_a_topic()
    {
        var topicOnly = new ServiceCollection();
        topicOnly.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.UseInMemoryCache();
            builder.FromKafkaTopic("entity.positions");
        });

        var clientOnly = new ServiceCollection();
        clientOnly.AddSingleton<IKafkaConsumerLoop>(new RecordingKafkaLoop());
        clientOnly.AddScopedDataSyncKafka(ConfigureClient);

        await using var topicProvider = topicOnly.BuildServiceProvider();
        await using var clientProvider = clientOnly.BuildServiceProvider();
        var guard = Assert.Single(topicProvider.GetServices<IHostedService>().OfType<KafkaSubscriptionGuard>());
        var host = Assert.Single(clientProvider.GetServices<IHostedService>().OfType<KafkaConsumerHost>());

        await Assert.ThrowsAsync<InvalidOperationException>(() => guard.StartAsync(CancellationToken.None));
        await host.StartAsync(CancellationToken.None);
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.ExecuteTask!);
    }

    [Fact]
    public void The_same_scope_accepts_one_topic_and_the_name_rejects_whitespace()
    {
        var duplicated = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddScopedDataSync<PositionSnapshot>(builder =>
            {
                builder.UseInMemoryCache();
                builder.FromKafkaTopic("entity.positions");
                builder.FromKafkaTopic("entity.positions.other");
            }));
        var blank = Assert.Throws<ArgumentException>(() =>
            new ServiceCollection().AddScopedDataSync<PositionSnapshot>(builder =>
            {
                builder.UseInMemoryCache();
                builder.FromKafkaTopic("entity positions");
            }));

        Assert.Contains("PositionSnapshot", duplicated.Message, StringComparison.Ordinal);
        Assert.Equal("topic", blank.ParamName);
    }

    [Fact]
    public async Task Accepted_event_is_what_the_reader_returns_and_a_replay_commits()
    {
        await using var provider = BuildPositions();
        var binding = SingleBinding(provider);
        var json = Envelope("inv-1", "positions", 1, new PositionSnapshot("PETR4", 10));

        var first = await binding.HandleAsync(provider, json, CancellationToken.None);
        var second = await binding.HandleAsync(provider, json, CancellationToken.None);
        var read = await provider.GetRequiredService<IScopeReader<PositionSnapshot>>()
            .ReadAsync("inv-1");

        Assert.Equal(KafkaCommitDecision.Commit, first);
        Assert.Equal(KafkaCommitDecision.Commit, second);
        Assert.Equal(DataFreshnessStatus.Fresh, read.Freshness.Status);
        Assert.Equal(new PositionSnapshot("PETR4", 10), read.Data);
        Assert.Equal(1, read.Freshness.Version);
    }

    [Fact]
    public async Task Invalid_payload_commits_after_recording_the_failure()
    {
        var handler = new RecordingErrorHandler();
        await using var provider = BuildPositions(handler);
        var binding = SingleBinding(provider);
        var broken = Envelope("inv-1", "positions", 1, payload: null);

        var decision = await binding.HandleAsync(provider, broken, CancellationToken.None);
        var read = await provider.GetRequiredService<IScopeReader<PositionSnapshot>>()
            .ReadAsync("inv-1");

        Assert.Equal(KafkaCommitDecision.Commit, decision);
        Assert.Equal(DataFreshnessStatus.Missing, read.Freshness.Status);
        Assert.Null(read.Data);
        Assert.Equal(1, read.Freshness.ConsecutiveFailures);
        Assert.Empty(handler.Failures);
    }

    [Fact]
    public async Task Unreadable_value_commits_without_a_failure_row()
    {
        await using var provider = BuildPositions();
        var binding = SingleBinding(provider);

        var decision = await binding.HandleAsync(provider, "not-json", CancellationToken.None);
        var read = await provider.GetRequiredService<IScopeReader<PositionSnapshot>>()
            .ReadAsync("inv-1");

        Assert.Equal(KafkaCommitDecision.Commit, decision);
        Assert.Equal(DataFreshnessStatus.Missing, read.Freshness.Status);
        Assert.Equal(0, read.Freshness.ConsecutiveFailures);
    }

    [Fact]
    public async Task Store_failure_leaves_the_offset_open()
    {
        var handler = new RecordingErrorHandler();
        var services = new ServiceCollection();
        services.AddSingleton<IScopedDataSyncErrorHandler>(handler);
        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.UseCacheProvider(new ThrowingCacheRegistration<PositionSnapshot>());
            builder.FromKafkaTopic("entity.positions");
        });
        await using var provider = services.BuildServiceProvider();
        var binding = SingleBinding(provider);

        var decision = await binding.HandleAsync(
            provider,
            Envelope("inv-1", "positions", 1, new PositionSnapshot("PETR4", 10)),
            CancellationToken.None);

        Assert.Equal(KafkaCommitDecision.Retry, decision);
        var failure = Assert.Single(handler.Failures);
        Assert.Equal(ScopedDataSyncFailureKind.KafkaMessageRetrying, failure.Kind);
        Assert.Equal("positions", failure.Scope);
        Assert.Equal("inv-1", failure.EntityId);
        Assert.Equal("kafka.apply", failure.Operation);
    }

    private static async Task<IReadOnlyList<string>> SubscribeAsync(bool kafkaBeforeScopes)
    {
        var loop = new RecordingKafkaLoop();
        var services = new ServiceCollection();
        services.AddSingleton<IKafkaConsumerLoop>(loop);
        if (kafkaBeforeScopes)
        {
            services.AddScopedDataSyncKafka(ConfigureClient);
        }

        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.UseInMemoryCache();
            builder.FromKafkaTopic("entity.positions");
        });
        services.AddScopedDataSync<AccountProfile>(builder =>
        {
            builder.WithScope("profile");
            builder.UseInMemoryCache();
            builder.FromKafkaTopic("entity.profile");
        });
        if (!kafkaBeforeScopes)
        {
            services.AddScopedDataSyncKafka(ConfigureClient);
        }

        await using var provider = services.BuildServiceProvider();
        var started = new List<IHostedService>();
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StartAsync(CancellationToken.None);
            started.Add(hosted);
        }

        try
        {
            foreach (var hosted in started.OfType<BackgroundService>())
            {
                await hosted.ExecuteTask!;
            }

            return loop.Topics.ToArray();
        }
        finally
        {
            foreach (var hosted in started.AsEnumerable().Reverse())
            {
                await hosted.StopAsync(CancellationToken.None);
            }
        }
    }

    private static ServiceProvider BuildPositions(IScopedDataSyncErrorHandler? handler = null)
    {
        var services = new ServiceCollection();
        if (handler is not null)
        {
            services.AddSingleton(handler);
        }

        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.UseInMemoryCache();
            builder.FromKafkaTopic("entity.positions");
        });
        return services.BuildServiceProvider();
    }

    private static IKafkaTopicBinding SingleBinding(IServiceProvider provider)
        => Assert.Single(provider.GetServices<IKafkaTopicBinding>());

    private static void ConfigureClient(ScopedDataSyncKafkaOptions options)
    {
        options.BootstrapServers = "localhost:9092";
        options.ClientId = "positions-api";
        options.GroupId = "positions-api";
    }

    private static string Envelope(string entityId, string scope, long version, PositionSnapshot? payload)
        => JsonSerializer.Serialize(
            new
            {
                entityId,
                scope,
                version,
                occurredAtUtc = DateTimeOffset.UtcNow,
                payload
            },
            ScopedDataSyncJson.Options);

    [Fact]
    public void Kafka_producer_keeps_acks_all_after_the_security_callback()
    {
        var config = KafkaProducerConfig.Create(new ScopedDataSyncKafkaProducerOptions
        {
            BootstrapServers = "kafka:9092",
            ClientId = "positions-producer",
            Configure = producer =>
            {
                producer.Acks = Acks.None;
                producer.SecurityProtocol = SecurityProtocol.SaslSsl;
                producer.SaslUsername = "outbox";
            }
        });

        Assert.Equal(Acks.All, config.Acks);
        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal("outbox", config.SaslUsername);
    }

    [Fact]
    public void Kafka_producer_registration_rejects_a_missing_client_and_a_second_publisher()
    {
        var missing = Assert.Throws<InvalidOperationException>(() =>
            new ServiceCollection().AddScopedDataSyncKafkaProducer(kafka => kafka.BootstrapServers = "kafka:9092"));
        var services = new ServiceCollection();
        services.AddSingleton<IOutboxKafkaPublisher>(new IdleOutboxPublisher());
        var duplicate = Assert.Throws<InvalidOperationException>(() =>
            services.AddScopedDataSyncKafkaProducer(kafka =>
            {
                kafka.BootstrapServers = "kafka:9092";
                kafka.ClientId = "positions-producer";
            }));

        Assert.Contains("BootstrapServers", missing.Message, StringComparison.Ordinal);
        Assert.Contains("already registered", duplicate.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_publisher_added_after_the_kafka_producer_fails_when_the_host_starts()
    {
        var services = new ServiceCollection();
        services.AddScopedDataSyncKafkaProducer(kafka =>
        {
            kafka.BootstrapServers = "kafka:9092";
            kafka.ClientId = "positions-producer";
        });
        services.AddSingleton<IOutboxKafkaPublisher>(new IdleOutboxPublisher());
        await using var provider = services.BuildServiceProvider();
        var guard = Assert.Single(provider.GetServices<IHostedService>().OfType<KafkaProducerRegistrationGuard>());

        await Assert.ThrowsAsync<InvalidOperationException>(() => guard.StartAsync(CancellationToken.None));
    }

    private sealed class IdleOutboxPublisher : IOutboxKafkaPublisher
    {
        public Task PublishAsync(OutboxKafkaMessage message, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class RecordingKafkaLoop : IKafkaConsumerLoop
    {
        public List<string> Topics { get; } = [];

        public Task RunAsync(
            ScopedDataSyncKafkaOptions options,
            IReadOnlyCollection<string> topics,
            Func<string, string?, CancellationToken, Task<KafkaCommitDecision>> dispatch,
            CancellationToken cancellationToken)
        {
            Topics.AddRange(topics);
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingCacheRegistration<TPayload> : CacheProviderRegistration<TPayload>
    {
        public override IProjectionStore<TPayload> CreateStore(
            IServiceProvider serviceProvider,
            ScopedDataSyncRegistrationContext context)
            => new ThrowingStore<TPayload>();
    }

    private sealed class ThrowingStore<TPayload> : IProjectionStore<TPayload>
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
}
