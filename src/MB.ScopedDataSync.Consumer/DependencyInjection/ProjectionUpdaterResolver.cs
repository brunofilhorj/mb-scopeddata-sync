using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Consumer.DependencyInjection;

public static class ProjectionUpdaterResolver
{
    public static IProjectionUpdater<TPayload> GetRequiredEventConsumerUpdater<TPayload>(
        this IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return services.GetRequiredKeyedService<IProjectionUpdater<TPayload>>(
            ScopedDataSyncServiceKeys.EventConsumer);
    }
}
