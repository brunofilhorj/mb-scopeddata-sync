using MB.ScopedDataSync.Abstractions.Reading;

namespace MB.ScopedDataSync.InMemoryExample.Positions;

public sealed class PortfolioService(IScopeReader<IReadOnlyList<PositionSnapshot>> reader)
{
    public async Task<IReadOnlyList<PositionSnapshot>> GetAsync(string entityId, CancellationToken cancellationToken)
    {
        var result = await reader.ReadAsync(entityId, cancellationToken);
        return result.Freshness.Status switch
        {
            DataFreshnessStatus.Fresh or DataFreshnessStatus.SyncImpaired => result.Data ?? [],
            DataFreshnessStatus.Stale => result.Data ?? [],
            _ => []
        };
    }
}
