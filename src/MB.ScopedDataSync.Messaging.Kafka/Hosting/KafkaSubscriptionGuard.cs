using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MB.ScopedDataSync.Messaging.Kafka.Hosting;

internal sealed class KafkaSubscriptionGuard(IServiceProvider services) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (services.GetService<IKafkaConsumerMarker>() is null)
        {
            throw new InvalidOperationException(
                "FromKafkaTopic requires AddScopedDataSyncKafka. The consumer does not open a broker by itself.");
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
