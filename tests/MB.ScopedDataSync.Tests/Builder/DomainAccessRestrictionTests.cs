using System.Reflection;
using MB.ScopedDataSync.Abstractions.Projection;
using MB.ScopedDataSync.Abstractions.Reading;
using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using MB.ScopedDataSync.Consumer.DependencyInjection;
using MB.ScopedDataSync.Tests.Support;
using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Tests.Builder;

public class DomainAccessRestrictionTests
{
    [Fact]
    public async Task Domain_service_reads_without_a_projection_updater()
    {
        var services = new ServiceCollection();
        services.AddScopedDataSync<PositionSnapshot>(builder =>
        {
            builder.WithScope("positions");
            builder.UseInMemoryCache();
        });

        Assert.DoesNotContain(
            services,
            descriptor => descriptor.ServiceType == typeof(IProjectionUpdater<PositionSnapshot>) && !descriptor.IsKeyedService);
        Assert.Contains(
            services,
            descriptor => descriptor.ServiceType == typeof(IProjectionUpdater<PositionSnapshot>)
                && Equals(descriptor.ServiceKey, ScopedDataSyncServiceKeys.EventConsumer));
        Assert.NotNull(typeof(IProjectionUpdater<PositionSnapshot>).GetCustomAttribute<EventConsumerOnlyAttribute>());
        Assert.Equal(
            ["ReadAsync"],
            typeof(IScopeReader<PositionSnapshot>).GetMethods().Select(method => method.Name).ToArray());

        using var provider = services.BuildServiceProvider();
        Assert.Null(provider.GetService<IProjectionUpdater<PositionSnapshot>>());
        Assert.Throws<InvalidOperationException>(() => ActivatorUtilities.CreateInstance<AccidentalDomainWriter>(provider));

        var updater = provider.GetRequiredEventConsumerUpdater<PositionSnapshot>();
        await updater.ApplyEventAsync(
            new ScopedEventEnvelope<PositionSnapshot>(
                "inv-1",
                "positions",
                7,
                new PositionSnapshot("PETR4", 7),
                DateTimeOffset.UtcNow),
            CancellationToken.None);

        var domain = ActivatorUtilities.CreateInstance<PortfolioDomainService>(provider);
        var read = await domain.GetAsync("inv-1");
        var reader = provider.GetRequiredService<IScopeReader<PositionSnapshot>>();

        Assert.Equal(new PositionSnapshot("PETR4", 7), read.Data);
        Assert.IsNotAssignableFrom<IProjectionUpdater<PositionSnapshot>>(reader);
    }

    private sealed class PortfolioDomainService(IScopeReader<PositionSnapshot> reader)
    {
        public Task<ReadResult<PositionSnapshot>> GetAsync(string entityId) => reader.ReadAsync(entityId);
    }

    private sealed class AccidentalDomainWriter(IProjectionUpdater<PositionSnapshot> updater)
    {
        public IProjectionUpdater<PositionSnapshot> Updater { get; } = updater;
    }
}
