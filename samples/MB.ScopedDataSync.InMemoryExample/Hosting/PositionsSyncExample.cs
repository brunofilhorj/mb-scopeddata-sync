using MB.ScopedDataSync.Abstractions.Reading;
using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using MB.ScopedDataSync.Consumer.DependencyInjection;
using MB.ScopedDataSync.InMemoryExample.Messaging;
using MB.ScopedDataSync.InMemoryExample.Positions;
using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Publishing;
using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.InMemoryExample.Hosting;

public sealed class PositionsSyncExample : IDisposable
{
    private readonly ServiceProvider _provider;
    private readonly InMemoryEventOutbox _outbox;
    private readonly InMemoryTopic _topic = new();

    public PositionsSyncExample()
    {
        _outbox = new InMemoryEventOutbox();
        var services = new ServiceCollection();
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<IEventOutbox>(_outbox);
        services.AddSingleton<IScopedEventPublisher>(serviceProvider =>
            new ScopedEventPublisher(
                serviceProvider.GetRequiredService<IEventOutbox>(),
                serviceProvider.GetRequiredService<TimeProvider>()));
        services.AddScopedDataSync<IReadOnlyList<PositionSnapshot>>(builder =>
        {
            builder.WithScope("positions");
            builder.UseInMemoryCache();
        });
        _provider = services.BuildServiceProvider();
    }

    public IScopedEventPublisher Publisher
        => _provider.GetRequiredService<IScopedEventPublisher>();

    public IScopeReader<IReadOnlyList<PositionSnapshot>> Reader
        => _provider.GetRequiredService<IScopeReader<IReadOnlyList<PositionSnapshot>>>();

    public IReadOnlyList<OutboxKafkaMessage> Messages => _topic.Messages;

    public async Task DeliverNewMessagesAsync(CancellationToken cancellationToken = default)
    {
        foreach (var published in _outbox.TakeUnpublished())
        {
            var message = OutboxKafkaMessage.Create(published);
            _topic.Append(message);
            await ApplyAsync(message, cancellationToken);
        }
    }

    public Task RedeliverAsync(OutboxKafkaMessage message, CancellationToken cancellationToken = default)
        => ApplyAsync(message, cancellationToken);

    public void Dispose() => _provider.Dispose();

    private async Task ApplyAsync(OutboxKafkaMessage message, CancellationToken cancellationToken)
    {
        var updater = _provider.GetRequiredEventConsumerUpdater<IReadOnlyList<PositionSnapshot>>();
        await updater.ApplyEventAsync(KafkaEnvelopeReader.Read<IReadOnlyList<PositionSnapshot>>(message), cancellationToken);
    }
}
