using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Publishing;

namespace MB.ScopedDataSync.Tests.Support;

internal sealed class RecordingOutboxTransaction : IOutboxTransaction
{
    private long _version;

    public List<string> Operations { get; } = [];

    public Task<long> IncrementVersionAsync(string entityId, string scope, CancellationToken cancellationToken)
    {
        Operations.Add("increment");
        _version++;
        return Task.FromResult(_version);
    }

    public Task InsertAsync(PublishedScopedEvent published, CancellationToken cancellationToken)
    {
        Operations.Add("insert");
        return Task.CompletedTask;
    }
}
