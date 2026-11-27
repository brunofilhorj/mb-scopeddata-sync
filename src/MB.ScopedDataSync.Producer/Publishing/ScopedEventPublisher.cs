using MB.ScopedDataSync.Producer.Outbox;

namespace MB.ScopedDataSync.Producer.Publishing;

public sealed class ScopedEventPublisher(IEventOutbox outbox, TimeProvider timeProvider)
    : IScopedEventPublisher
{
    public async Task<PublishedScopedEvent> PublishAsync<TPayload>(
        PublishEntityEventRequest<TPayload> request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        SyncNameRules.ThrowIfInvalidEntityId(request.EntityId);
        SyncNameRules.ThrowIfInvalidScope(request.Scope);
        if (string.IsNullOrWhiteSpace(request.Topic) || request.Topic.AsSpan().ContainsAny("{} \t\r\n"))
        {
            throw new ArgumentException("A Kafka topic is required and cannot contain braces or whitespace.", nameof(request));
        }

        var payloadJson = PayloadNormalizer.ToJson(request.Payload);
        var occurredAt = (request.OccurredAtUtc ?? timeProvider.GetUtcNow()).ToUniversalTime();
        var draft = new NewOutboxEvent(
            Guid.CreateVersion7(),
            request.EntityId,
            request.Scope,
            request.Topic.Trim(),
            payloadJson,
            occurredAt);

        return await outbox.AppendAsync(draft, cancellationToken).ConfigureAwait(false);
    }
}
