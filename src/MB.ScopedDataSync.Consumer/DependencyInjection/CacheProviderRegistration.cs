using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Consumer.DependencyInjection;

/// <summary>
/// Ponto de extensão de um provedor de cache. O builder aceita no máximo uma instância:
/// <c>UseInMemoryCache</c> ou <c>UseRedisCache</c>. Não há cadeia InMemory, Redis e banco.
/// </summary>
public abstract class CacheProviderRegistration<TPayload>
{
    public virtual void ConfigureServices(IServiceCollection services, ScopedDataSyncRegistrationContext context)
    {
    }

    public abstract IProjectionStore<TPayload> CreateStore(
        IServiceProvider serviceProvider,
        ScopedDataSyncRegistrationContext context);
}
