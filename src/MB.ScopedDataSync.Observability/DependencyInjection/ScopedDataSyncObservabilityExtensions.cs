using MB.ScopedDataSync.Observability.Metrics;
using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Observability.DependencyInjection;

public static class ScopedDataSyncObservabilityExtensions
{
    public static IServiceCollection AddScopedDataSyncObservability(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        for (var index = services.Count - 1; index >= 0; index--)
        {
            if (services[index].ServiceType == typeof(IScopedDataSyncInstrumentation))
            {
                services.RemoveAt(index);
            }
        }

        services.AddSingleton<IScopedDataSyncInstrumentation, MetricsScopedDataSyncInstrumentation>();
        return services;
    }
}
