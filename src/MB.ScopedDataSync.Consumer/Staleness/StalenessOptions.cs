namespace MB.ScopedDataSync.Consumer.Staleness;

public sealed class StalenessOptions
{
    public TimeSpan StaleAfter { get; set; } = TimeSpan.FromMinutes(5);

    public TimeSpan AlertCooldown { get; set; } = TimeSpan.FromMinutes(15);
}
