namespace MB.ScopedDataSync.Caching.InMemory.DependencyInjection;

public static class InMemoryScopedDataSyncBuilderExtensions
{
    public static ScopedDataSyncBuilder<TPayload> UseInMemoryCache<TPayload>(
        this ScopedDataSyncBuilder<TPayload> builder,
        Action<InMemoryCacheOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var options = new InMemoryCacheOptions();
        configure?.Invoke(options);
        return builder.UseCacheProvider(new InMemoryCacheProviderRegistration<TPayload>(options));
    }
}
