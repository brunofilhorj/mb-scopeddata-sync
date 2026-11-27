using MB.ScopedDataSync.Producer.Outbox;

namespace MB.ScopedDataSync.InMemoryExample.Messaging;

internal sealed class InMemoryTopic
{
    private readonly List<OutboxKafkaMessage> _messages = [];

    public void Append(OutboxKafkaMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        lock (_messages)
        {
            _messages.Add(message);
        }
    }

    public IReadOnlyList<OutboxKafkaMessage> Messages
    {
        get
        {
            lock (_messages)
            {
                return _messages.ToArray();
            }
        }
    }
}
