using System.Collections.Concurrent;

namespace MB.ScopedDataSync.Consumer.Staleness;

public sealed class InMemoryAlertCooldownGate : IAlertCooldownGate
{
    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastAlertAt = new(StringComparer.Ordinal);

    public bool TryEnter(string key, TimeSpan cooldown, DateTimeOffset now)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentOutOfRangeException.ThrowIfLessThan(cooldown, TimeSpan.Zero);

        while (true)
        {
            if (!_lastAlertAt.TryGetValue(key, out var last))
            {
                if (_lastAlertAt.TryAdd(key, now))
                {
                    return true;
                }

                continue;
            }

            if (now - last < cooldown)
            {
                return false;
            }

            if (_lastAlertAt.TryUpdate(key, now, last))
            {
                return true;
            }
        }
    }

    public void Undo(string key, DateTimeOffset enteredAt)
    {
        _lastAlertAt.TryUpdate(key, DateTimeOffset.MinValue, enteredAt);
    }
}
