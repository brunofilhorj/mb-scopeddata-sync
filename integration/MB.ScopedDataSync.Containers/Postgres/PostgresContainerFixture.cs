using MB.ScopedDataSync.Containers.Docker;
using Testcontainers.PostgreSql;

namespace MB.ScopedDataSync.Containers.Postgres;

public sealed class PostgresContainerFixture : IAsyncLifetime
{
    public const string CollectionName = "postgres";

    private PostgreSqlContainer? _container;

    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        if (!DockerAvailability.IsAvailable)
        {
            return;
        }

        _container = new PostgreSqlBuilder("postgres:17").Build();
        await _container.StartAsync();
        ConnectionString = _container.GetConnectionString();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }
}

[CollectionDefinition(PostgresContainerFixture.CollectionName)]
public sealed class PostgresCollection : ICollectionFixture<PostgresContainerFixture>;
