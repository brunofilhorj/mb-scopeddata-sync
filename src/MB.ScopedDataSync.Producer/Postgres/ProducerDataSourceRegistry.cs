using System.Collections.Concurrent;
using MB.ScopedDataSync.Producer.DependencyInjection;
using Npgsql;

namespace MB.ScopedDataSync.Producer.Postgres;

internal sealed class ProducerDatabase(NpgsqlDataSource dataSource)
{
    public NpgsqlDataSource DataSource { get; } = dataSource;
}

internal sealed class ProducerDataSourceRegistry : IDisposable
{
    private readonly ConcurrentDictionary<string, NpgsqlDataSource> _owned = new(StringComparer.Ordinal);

    public NpgsqlDataSource Resolve(ScopedDataSyncProducerOptions options)
    {
        if (options.DataSource is not null)
        {
            return options.DataSource;
        }

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException("The producer requires a connection string or an NpgsqlDataSource.");
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
