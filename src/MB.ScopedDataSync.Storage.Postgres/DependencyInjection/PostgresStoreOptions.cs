using Npgsql;

namespace MB.ScopedDataSync.Storage.Postgres.DependencyInjection;

public sealed class PostgresStoreOptions
{
    public string? ConnectionString { get; set; }

    public NpgsqlDataSource? DataSource { get; set; }

    public bool EnsureSchemaOnStartup { get; set; } = true;
}
