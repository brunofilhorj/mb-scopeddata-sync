using Microsoft.Extensions.Hosting;
using Npgsql;

namespace MB.ScopedDataSync.Producer.Postgres;

internal sealed class PostgresProducerSchemaInitializer(NpgsqlDataSource dataSource) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(PostgresOutboxSql.EnsureSchema);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
