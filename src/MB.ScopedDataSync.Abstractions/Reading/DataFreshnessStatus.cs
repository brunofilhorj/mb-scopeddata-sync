namespace MB.ScopedDataSync.Abstractions.Reading;

public enum DataFreshnessStatus
{
    Missing = 0,
    Fresh = 1,
    Stale = 2,
    SyncImpaired = 3
}
