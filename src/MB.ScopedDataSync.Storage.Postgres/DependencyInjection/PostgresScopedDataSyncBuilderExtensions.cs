namespace MB.ScopedDataSync.Storage.Postgres.DependencyInjection;

public static class PostgresScopedDataSyncBuilderExtensions
{
    public static ScopedDataSyncBuilder<TPayload> UsePostgresStore<TPayload>(
        this ScopedDataSyncBuilder<TPayload> builder,
        Action<PostgresStoreOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new PostgresStoreOptions();
        configure(options);
        if (options.DataSource is null && string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException("PostgreSQL store requires a connection string or an NpgsqlDataSource.");
        }

        return builder.UseDatabaseStore(new PostgresStoreProviderRegistration<TPayload>(options));
    }
}
