using System.Collections.Concurrent;
using MB.ScopedDataSync.Storage.Postgres.DependencyInjection;
using Npgsql;

namespace MB.ScopedDataSync.Storage.Postgres.Store;

internal sealed class PostgresDataSourceRegistry : IDisposable
{
    private readonly ConcurrentDictionary<string, NpgsqlDataSource> _owned = new(StringComparer.Ordinal);

    public NpgsqlDataSource Resolve(PostgresStoreOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.DataSource is not null)
        {
            return options.DataSource;
        }

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException("PostgreSQL store requires a connection string or an NpgsqlDataSource.");
        }

        return _owned.GetOrAdd(options.ConnectionString, static connectionString => NpgsqlDataSource.Create(connectionString));
    }

    public void Dispose()
    {
        foreach (var dataSource in _owned.Values)
        {
            dataSource.Dispose();
        }

        _owned.Clear();
    }
}
