using MB.ScopedDataSync.Caching.Redis.Store;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MB.ScopedDataSync.Caching.Redis.DependencyInjection;

internal sealed class RedisCacheProviderRegistration<TPayload>(RedisCacheOptions options) : CacheProviderRegistration<TPayload>
{
    public override void ConfigureServices(IServiceCollection services, ScopedDataSyncRegistrationContext context)
        => services.TryAddSingleton<RedisMultiplexerRegistry>();

    public override IProjectionStore<TPayload> CreateStore(
        IServiceProvider serviceProvider,
        ScopedDataSyncRegistrationContext context)
    {
        var multiplexer = serviceProvider.GetRequiredService<RedisMultiplexerRegistry>().Resolve(options);
        return new RedisProjectionStore<TPayload>(multiplexer, options, context.Scope);
    }
}
