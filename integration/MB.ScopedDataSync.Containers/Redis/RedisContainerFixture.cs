using MB.ScopedDataSync.Containers.Docker;
using Testcontainers.Redis;

namespace MB.ScopedDataSync.Containers.Redis;

public sealed class RedisContainerFixture : IAsyncLifetime
{
    private RedisContainer? _container;

    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        if (!DockerAvailability.IsAvailable)
        {
            return;
        }

        _container = new RedisBuilder("redis:7.4").Build();
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
