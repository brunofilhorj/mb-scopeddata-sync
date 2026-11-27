using MB.ScopedDataSync.Storage.Postgres.Store;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MB.ScopedDataSync.Storage.Postgres.DependencyInjection;

internal sealed class PostgresStoreProviderRegistration<TPayload>(PostgresStoreOptions options)
    : DatabaseStoreRegistration<TPayload>
{
    public override void ConfigureServices(IServiceCollection services, ScopedDataSyncRegistrationContext context)
    {
        services.TryAddSingleton<PostgresDataSourceRegistry>();
        if (!options.EnsureSchemaOnStartup)
        {
            return;
        }

        services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(serviceProvider =>
            new PostgresProjectionSchemaInitializer(
                serviceProvider.GetRequiredService<PostgresDataSourceRegistry>().Resolve(options)));
    }

    public override IProjectionStore<TPayload> CreateStore(
        IServiceProvider serviceProvider,
        ScopedDataSyncRegistrationContext context)
        => new PostgresProjectionStore<TPayload>(
            serviceProvider.GetRequiredService<PostgresDataSourceRegistry>().Resolve(options),
            context.Scope);
}
