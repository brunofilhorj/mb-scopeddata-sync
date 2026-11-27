using MB.ScopedDataSync.Messaging.Kafka.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace MB.ScopedDataSync.Messaging.Kafka.Dispatch;

internal interface IKafkaTopicBinding
{
    string Topic { get; }

    Task<KafkaCommitDecision> HandleAsync(
        IServiceProvider services,
        string? value,
        CancellationToken cancellationToken);
}

internal sealed class KafkaTopicBinding<TPayload>(string topic, string scope) : IKafkaTopicBinding
{
    public string Topic { get; } = topic;

    public async Task<KafkaCommitDecision> HandleAsync(
        IServiceProvider services,
        string? value,
        CancellationToken cancellationToken)
    {
        var logger = services.GetService<ILoggerFactory>()?.CreateLogger<KafkaTopicBinding<TPayload>>()
            ?? NullLogger<KafkaTopicBinding<TPayload>>.Instance;
        var errors = services.GetService<IScopedDataSyncErrorHandler>() ?? NullScopedDataSyncErrorHandler.Instance;

        if (string.IsNullOrWhiteSpace(value))
        {
            logger.LogWarning(
                "Kafka topic {Topic} delivered an empty value. The offset will be committed.",
                Topic);
            return KafkaCommitDecision.Commit;
        }

        ScopedEventEnvelope<TPayload> envelope;
        try
        {
            envelope = KafkaEnvelopeReader.Read<TPayload>(value);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return await RejectAsync(services, logger, errors, KafkaEnvelopeReader.TryReadEntityId(value), exception, cancellationToken)
                .ConfigureAwait(false);
        }

        if (envelope.Version <= 0)
        {
            return await RejectAsync(
                services,
                logger,
                errors,
                envelope.EntityId,
                new ArgumentOutOfRangeException(nameof(envelope), envelope.Version, "Version must be greater than zero."),
                cancellationToken).ConfigureAwait(false);
        }

        if (!string.Equals(envelope.Scope, scope, StringComparison.Ordinal))
        {
            return await RejectAsync(
                services,
                logger,
                errors,
                envelope.EntityId,
                new InvalidOperationException(
                    $"The event scope '{envelope.Scope}' does not match the registered scope '{scope}'."),
                cancellationToken).ConfigureAwait(false);
        }

        try
        {
            await services.GetRequiredEventConsumerUpdater<TPayload>()
                .ApplyEventAsync(envelope, cancellationToken)
                .ConfigureAwait(false);
            return KafkaCommitDecision.Commit;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            logger.LogError(
                exception,
                "Kafka topic {Topic} for {EntityId} was not stored. The offset stays open.",
                Topic,
                envelope.EntityId);
            await ScopedDataSyncErrorNotifications.NotifyAsync(
                errors,
                logger,
                new ScopedDataSyncFailure(
                    ScopedDataSyncFailureKind.KafkaMessageRetrying,
                    scope,
                    envelope.EntityId,
                    "kafka.apply",
                    exception),
                cancellationToken).ConfigureAwait(false);
            return KafkaCommitDecision.Retry;
        }
    }

    private async Task<KafkaCommitDecision> RejectAsync(
        IServiceProvider services,
        ILogger logger,
        IScopedDataSyncErrorHandler errors,
        string? entityId,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (entityId is not null)
        {
            try
            {
                var reason = string.IsNullOrWhiteSpace(exception.Message)
                    ? "The Kafka payload was rejected."
                    : exception.Message;
                var time = services.GetService<TimeProvider>() ?? TimeProvider.System;
                await services.GetRequiredEventConsumerUpdater<TPayload>()
                    .RecordSyncFailureAsync(
                        new SyncFailure(entityId, reason, time.GetUtcNow()),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception recordException) when (recordException is not OperationCanceledException)
            {
                logger.LogError(
                    recordException,
                    "Kafka topic {Topic} for {EntityId} was rejected and the failure record was not stored. The offset stays open.",
                    Topic,
                    entityId);
                await ScopedDataSyncErrorNotifications.NotifyAsync(
                    errors,
                    logger,
                    new ScopedDataSyncFailure(
                        ScopedDataSyncFailureKind.KafkaMessageRetrying,
                        scope,
                        entityId,
                        "kafka.record_failure",
                        recordException),
                    cancellationToken).ConfigureAwait(false);
                return KafkaCommitDecision.Retry;
            }
        }

        logger.LogWarning(
            exception,
            "Kafka topic {Topic} was rejected. The offset will be committed.",
            Topic);
        return KafkaCommitDecision.Commit;
    }
}
