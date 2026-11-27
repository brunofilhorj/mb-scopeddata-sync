using MB.ScopedDataSync.Producer.Outbox;

namespace MB.ScopedDataSync.Containers.Support;

internal sealed class RecordingOutboxPublisher : IOutboxKafkaPublisher
{
    public List<OutboxKafkaMessage> Messages { get; } = [];

    public Task PublishAsync(OutboxKafkaMessage message, CancellationToken cancellationToken)
    {
        Messages.Add(message);
        return Task.CompletedTask;
    }
}
