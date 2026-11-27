using MB.ScopedDataSync.Producer.Outbox;
using Microsoft.Extensions.Hosting;

namespace MB.ScopedDataSync.Messaging.Kafka.Hosting;

internal interface IKafkaProducerMarker;

internal sealed class KafkaProducerRegistrationGuard(IEnumerable<IOutboxKafkaPublisher> publishers) : IHostedService
{
    private readonly IReadOnlyList<IOutboxKafkaPublisher> _publishers = publishers.ToArray();

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_publishers.Count != 1)
        {
            throw new InvalidOperationException(
                "Register a single IOutboxKafkaPublisher. AddScopedDataSyncKafkaProducer already registers one.");
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
