using Npgsql;

namespace MB.ScopedDataSync.Producer.DependencyInjection;

public sealed class ScopedDataSyncProducerOptions
{
    public string? ConnectionString { get; set; }

    public NpgsqlDataSource? DataSource { get; set; }

    public bool EnsureSchemaOnStartup { get; set; } = true;
}
