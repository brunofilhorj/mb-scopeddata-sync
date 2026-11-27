using MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;
using MB.ScopedDataSync.Messaging.Kafka.Dispatch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MB.ScopedDataSync.Messaging.Kafka.Hosting;

internal interface IKafkaConsumerMarker
{
    ScopedDataSyncKafkaOptions Options { get; }
}

internal sealed class KafkaConsumerMarker(ScopedDataSyncKafkaOptions options) : IKafkaConsumerMarker
{
    public ScopedDataSyncKafkaOptions Options { get; } = options;
}

internal sealed class KafkaConsumerHost(
    IServiceScopeFactory scopes,
    IKafkaConsumerMarker marker,
    IEnumerable<IKafkaTopicBinding> bindings,
    IKafkaConsumerLoop loop) : BackgroundService
{
    private readonly IReadOnlyList<IKafkaTopicBinding> _bindings = bindings.ToArray();

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_bindings.Count == 0)
        {
            throw new InvalidOperationException("AddScopedDataSyncKafka requires at least one FromKafkaTopic.");
        }

        var duplicate = _bindings
            .GroupBy(binding => binding.Topic, StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicate is not null)
        {
            throw new InvalidOperationException($"Kafka topic '{duplicate.Key}' is registered more than once.");
        }

        var topics = _bindings.Select(binding => binding.Topic).ToArray();
        return loop.RunAsync(
            marker.Options,
            topics,
            (topic, value, cancellationToken) => DispatchAsync(topic, value, cancellationToken),
            stoppingToken);
    }

    private async Task<KafkaCommitDecision> DispatchAsync(
        string topic,
        string? value,
        CancellationToken cancellationToken)
    {
        var binding = _bindings.First(candidate => string.Equals(candidate.Topic, topic, StringComparison.Ordinal));
        await using var scope = scopes.CreateAsyncScope();
        return await binding.HandleAsync(scope.ServiceProvider, value, cancellationToken).ConfigureAwait(false);
    }
}
