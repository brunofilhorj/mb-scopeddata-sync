namespace MB.ScopedDataSync.Tests.Support;

internal sealed class FixedTimeProvider : TimeProvider
{
    public FixedTimeProvider(DateTimeOffset utcNow) => UtcNow = utcNow;

    public DateTimeOffset UtcNow { get; set; }

    public override DateTimeOffset GetUtcNow() => UtcNow;
}
