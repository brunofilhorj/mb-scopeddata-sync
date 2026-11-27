using MB.ScopedDataSync.Abstractions.Errors;
using MB.ScopedDataSync.Producer.Hosting;
using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Postgres;
using MB.ScopedDataSync.Producer.Publishing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MB.ScopedDataSync.Producer.DependencyInjection;

public static class ScopedDataSyncProducerExtensions
{
    public static IServiceCollection AddScopedDataSyncProducer(
        this IServiceCollection services,
        Action<ScopedDataSyncProducerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        var options = new ScopedDataSyncProducerOptions();
        configure(options);
        if (options.DataSource is null && string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException("The producer requires a connection string or an NpgsqlDataSource.");
        }

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<ProducerDataSourceRegistry>();
        services.AddSingleton(serviceProvider => new ProducerDatabase(
            serviceProvider.GetRequiredService<ProducerDataSourceRegistry>().Resolve(options)));
        services.AddSingleton<IEventOutbox>(serviceProvider =>
            new PostgresEventOutbox(serviceProvider.GetRequiredService<ProducerDatabase>().DataSource));
        services.AddSingleton<IScopedEventPublisher>(serviceProvider =>
            new ScopedEventPublisher(
                serviceProvider.GetRequiredService<IEventOutbox>(),
                serviceProvider.GetRequiredService<TimeProvider>()));

        if (options.EnsureSchemaOnStartup)
        {
            services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService>(serviceProvider =>
                new PostgresProducerSchemaInitializer(serviceProvider.GetRequiredService<ProducerDatabase>().DataSource));
        }

        return services;
    }

    /// <summary>
    /// Sobe o hosted service que publica a outbox. Lote vazio espera <see cref="OutboxRelayOptions.IdleDelay"/>.
    /// Falha de reserva ou de envio vira <see cref="ScopedDataSyncFailureKind.OutboxRelayFaulted"/> e o loop continua.
    /// A segunda chamada lança.
    /// </summary>
    public static IServiceCollection AddScopedDataSyncOutboxRelay(
        this IServiceCollection services,
        Action<OutboxRelayOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (services.Any(descriptor => descriptor.ServiceType == typeof(OutboxRelayMarker)))
        {
            throw new InvalidOperationException("Outbox relay is already registered.");
        }

        var options = new OutboxRelayOptions();
        configure?.Invoke(options);
        if (options.BatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.BatchSize, "Batch size must be positive.");
        }

        if (options.IdleDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.IdleDelay, "Idle delay cannot be negative.");
        }

        services.AddSingleton(new OutboxRelayMarker());
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IScopedDataSyncErrorHandler>(NullScopedDataSyncErrorHandler.Instance);
        services.AddSingleton(serviceProvider => new PostgresOutboxRelay(
            serviceProvider.GetRequiredService<ProducerDatabase>().DataSource,
            serviceProvider.GetRequiredService<IOutboxKafkaPublisher>(),
            serviceProvider.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IHostedService>(serviceProvider => new OutboxRelayHost(
            serviceProvider.GetRequiredService<PostgresOutboxRelay>(),
            options,
            serviceProvider.GetRequiredService<IScopedDataSyncErrorHandler>(),
            serviceProvider.GetService<ILoggerFactory>()?.CreateLogger<OutboxRelayHost>()
                ?? NullLogger<OutboxRelayHost>.Instance,
            serviceProvider.GetRequiredService<TimeProvider>()));
        return services;
    }

    private sealed class OutboxRelayMarker;
}
