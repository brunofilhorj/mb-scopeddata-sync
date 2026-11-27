using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MB.ScopedDataSync.Containers.Support;

internal static class HostedServiceRunner
{
    public static async Task StartAsync(IServiceProvider provider, CancellationToken cancellationToken)
    {
        foreach (var service in provider.GetServices<IHostedService>())
        {
            await service.StartAsync(cancellationToken);
        }
    }

    public static async Task StopAsync(IServiceProvider provider, CancellationToken cancellationToken)
    {
        foreach (var service in provider.GetServices<IHostedService>().Reverse())
        {
            await service.StopAsync(cancellationToken);
        }
    }
}
