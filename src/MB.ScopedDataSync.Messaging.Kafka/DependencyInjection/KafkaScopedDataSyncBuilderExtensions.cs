using MB.ScopedDataSync.Messaging.Kafka.Dispatch;
using MB.ScopedDataSync.Messaging.Kafka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;

public static class KafkaScopedDataSyncBuilderExtensions
{
    /// <summary>
    /// Liga este escopo a um tópico. O hosted service de <c>AddScopedDataSyncKafka</c> assina o nome
    /// e aplica o envelope no updater deste <c>TPayload</c>. Um escopo aceita um tópico.
    /// </summary>
    public static ScopedDataSyncBuilder<TPayload> FromKafkaTopic<TPayload>(
        this ScopedDataSyncBuilder<TPayload> builder,
        string topic)
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (string.IsNullOrWhiteSpace(topic) || topic.AsSpan().ContainsAny("{} \t\r\n"))
        {
            throw new ArgumentException(
                "A Kafka topic is required and cannot contain braces or whitespace.",
                nameof(topic));
        }

        return builder.UseEventSource((services, context) =>
        {
            if (services.Any(descriptor => descriptor.ImplementationInstance is KafkaTopicBinding<TPayload>))
            {
                throw new InvalidOperationException(
                    $"Kafka topic for {typeof(TPayload).Name} is already registered.");
            }

            services.AddSingleton<IKafkaTopicBinding>(new KafkaTopicBinding<TPayload>(topic.Trim(), context.Scope));
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, KafkaSubscriptionGuard>());
        });
    }
}
