using MB.ScopedDataSync.Messaging.Kafka.Dispatch;
using MB.ScopedDataSync.Messaging.Kafka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;

public static class ScopedDataSyncKafkaExtensions
{
    /// <summary>
    /// Sobe um hosted service que assina os tópicos de <c>FromKafkaTopic</c>.
    /// A ordem em relação a <c>AddScopedDataSync&lt;T&gt;</c> não importa: a lista é lida na subida do host.
    /// Descarte por versão confirma o offset. Falha ao gravar pausa só a partição da mensagem e tenta de novo.
    /// Falha de consume ou commit mantém o processo no ar. Payload inválido registra falha de sync e confirma.
    /// </summary>
    public static IServiceCollection AddScopedDataSyncKafka(
        this IServiceCollection services,
        Action<ScopedDataSyncKafkaOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        if (services.Any(descriptor => descriptor.ServiceType == typeof(IKafkaConsumerMarker)))
        {
            throw new InvalidOperationException("Kafka consumer is already registered.");
        }

        var options = new ScopedDataSyncKafkaOptions();
        configure(options);
        if (string.IsNullOrWhiteSpace(options.BootstrapServers)
            || string.IsNullOrWhiteSpace(options.ClientId)
            || string.IsNullOrWhiteSpace(options.GroupId))
        {
            throw new InvalidOperationException("Kafka consumer requires BootstrapServers, ClientId and GroupId.");
        }

        services.AddSingleton<IKafkaConsumerMarker>(new KafkaConsumerMarker(options));
        services.TryAddSingleton<IScopedDataSyncErrorHandler>(NullScopedDataSyncErrorHandler.Instance);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IKafkaConsumerLoop>(serviceProvider => new ConfluentKafkaConsumerLoop(
            serviceProvider.GetService<ILoggerFactory>()?.CreateLogger<ConfluentKafkaConsumerLoop>()
                ?? NullLogger<ConfluentKafkaConsumerLoop>.Instance,
            serviceProvider.GetRequiredService<IScopedDataSyncErrorHandler>(),
            serviceProvider.GetRequiredService<TimeProvider>()));
        services.AddSingleton<IHostedService>(serviceProvider => new KafkaConsumerHost(
            serviceProvider.GetRequiredService<IServiceScopeFactory>(),
            serviceProvider.GetRequiredService<IKafkaConsumerMarker>(),
            serviceProvider.GetServices<IKafkaTopicBinding>(),
            serviceProvider.GetRequiredService<IKafkaConsumerLoop>()));
        return services;
    }
}
