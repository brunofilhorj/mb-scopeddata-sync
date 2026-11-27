using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Consumer.DependencyInjection;

/// <summary>
/// Ponto de extensão de um store de banco, relacional ou NoSQL.
/// O builder aceita no máximo uma instância. <c>UsePostgresStore</c> é a implementação PostgreSQL.
/// </summary>
public abstract class DatabaseStoreRegistration<TPayload>
{
    public virtual void ConfigureServices(IServiceCollection services, ScopedDataSyncRegistrationContext context)
    {
    }

    public abstract IProjectionStore<TPayload> CreateStore(
        IServiceProvider serviceProvider,
        ScopedDataSyncRegistrationContext context);
}
