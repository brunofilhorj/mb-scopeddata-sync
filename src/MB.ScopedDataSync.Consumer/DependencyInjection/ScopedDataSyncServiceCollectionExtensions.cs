using Microsoft.Extensions.DependencyInjection;

namespace MB.ScopedDataSync.Consumer.DependencyInjection;

public static class ScopedDataSyncServiceCollectionExtensions
{
    public static IServiceCollection AddScopedDataSync<TPayload>(
        this IServiceCollection services,
        Action<ScopedDataSyncBuilder<TPayload>> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        if (services.Any(descriptor => descriptor.ServiceType == typeof(IScopeReader<TPayload>)))
        {
            throw new InvalidOperationException(
                $"Scoped data sync for {typeof(TPayload).Name} is already registered.");
        }

        var builder = new ScopedDataSyncBuilder<TPayload>();
        configure(builder);
        builder.Register(services);
        return services;
    }
}
