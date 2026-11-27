using MB.ScopedDataSync.Caching.InMemory.Store;
using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Caching.InMemory.DependencyInjection;

internal sealed class InMemoryCacheProviderRegistration<TPayload>(InMemoryCacheOptions options)
    : CacheProviderRegistration<TPayload>
{
    public override void ConfigureServices(IServiceCollection services, ScopedDataSyncRegistrationContext context)
        => services.AddMemoryCache();

    public override IProjectionStore<TPayload> CreateStore(
        IServiceProvider serviceProvider,
        ScopedDataSyncRegistrationContext context)
        => new InMemoryProjectionStore<TPayload>(
            serviceProvider.GetRequiredService<Microsoft.Extensions.Caching.Memory.IMemoryCache>(),
            context.Scope,
            options);
}
