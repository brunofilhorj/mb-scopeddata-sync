using MB.ScopedDataSync.Storage.Postgres.Sql;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace MB.ScopedDataSync.Storage.Postgres.Store;

internal sealed class PostgresProjectionSchemaInitializer(NpgsqlDataSource dataSource) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(PostgresProjectionSql.EnsureSchema);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
