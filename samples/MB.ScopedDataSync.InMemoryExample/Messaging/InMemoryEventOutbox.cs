using System.Collections.Concurrent;
using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Publishing;

namespace MB.ScopedDataSync.InMemoryExample.Messaging;

internal sealed class InMemoryEventOutbox : IEventOutbox
{
    private readonly ConcurrentDictionary<(string EntityId, string Scope), long> _versions = new();
    private readonly ConcurrentDictionary<(string EntityId, string Scope), object> _gates = new();
    private readonly List<PublishedScopedEvent> _events = [];
    private readonly HashSet<Guid> _published = [];

    public Task<PublishedScopedEvent> AppendAsync(NewOutboxEvent newEvent, CancellationToken cancellationToken)
    {
        var key = (newEvent.EntityId, newEvent.Scope);
        var gate = _gates.GetOrAdd(key, static _ => new object());
        lock (gate)
        {
            var version = _versions.AddOrUpdate(key, 1, static (_, current) => current + 1);
            var published = new PublishedScopedEvent(
                newEvent.EventId,
                newEvent.EntityId,
                newEvent.EntityId,
                newEvent.Scope,
                newEvent.Topic,
                version,
                newEvent.PayloadJson,
                newEvent.OccurredAtUtc);
            lock (_events)
            {
                _events.Add(published);
            }

            return Task.FromResult(published);
        }
    }

    public PublishedScopedEvent[] TakeUnpublished()
    {
        lock (_events)
        {
            var pending = new List<PublishedScopedEvent>();
            foreach (var item in _events)
            {
                if (_published.Add(item.EventId))
                {
                    pending.Add(item);
                }
            }

            return pending.ToArray();
        }
    }
}
