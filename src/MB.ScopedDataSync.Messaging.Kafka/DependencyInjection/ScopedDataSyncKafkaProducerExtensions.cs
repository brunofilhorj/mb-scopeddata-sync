using MB.ScopedDataSync.Messaging.Kafka.Hosting;
using MB.ScopedDataSync.Messaging.Kafka.Publishing;
using MB.ScopedDataSync.Producer.Outbox;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;

public static class ScopedDataSyncKafkaProducerExtensions
{
    /// <summary>
    /// Registra o cliente Confluent de <c>IOutboxKafkaPublisher</c>.
    /// A chave e o valor saem de <c>OutboxKafkaMessage</c>. <c>Acks</c> permanece <c>All</c>.
    /// Um <c>IOutboxKafkaPublisher</c> já registrado, ou uma segunda chamada, falha na composição.
    /// </summary>
    public static IServiceCollection AddScopedDataSyncKafkaProducer(
        this IServiceCollection services,
        Action<ScopedDataSyncKafkaProducerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(descriptor => descriptor.ServiceType == typeof(IKafkaProducerMarker)))
        {
            throw new InvalidOperationException("Kafka producer is already registered.");
        }

        if (services.Any(descriptor => descriptor.ServiceType == typeof(IOutboxKafkaPublisher)))
        {
            throw new InvalidOperationException(
                "IOutboxKafkaPublisher is already registered. AddScopedDataSyncKafkaProducer registers the Confluent client.");
        }

        var options = new ScopedDataSyncKafkaProducerOptions();
        configure(options);
        if (string.IsNullOrWhiteSpace(options.BootstrapServers) || string.IsNullOrWhiteSpace(options.ClientId))
        {
            throw new InvalidOperationException("Kafka producer requires BootstrapServers and ClientId.");
        }

        services.AddSingleton<IKafkaProducerMarker, KafkaProducerMarker>();
        services.AddSingleton<IOutboxKafkaPublisher>(_ => new ConfluentOutboxPublisher(options));
        services.AddSingleton<IHostedService>(serviceProvider =>
            new KafkaProducerRegistrationGuard(serviceProvider.GetServices<IOutboxKafkaPublisher>()));
        return services;
    }

    private sealed class KafkaProducerMarker : IKafkaProducerMarker;
}
