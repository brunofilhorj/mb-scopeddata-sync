using MB.ScopedDataSync.Abstractions.Errors;
using MB.ScopedDataSync.Producer.DependencyInjection;
using MB.ScopedDataSync.Producer.Outbox;
using MB.ScopedDataSync.Producer.Postgres;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MB.ScopedDataSync.Producer.Hosting;

internal sealed class OutboxRelayHost(
    PostgresOutboxRelay relay,
    OutboxRelayOptions options,
    IScopedDataSyncErrorHandler errors,
    ILogger<OutboxRelayHost> logger,
    TimeProvider time) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
        => new OutboxRelayLoop().RunAsync(
            cancellationToken => relay.DispatchAsync(options.BatchSize, cancellationToken),
            options.IdleDelay,
            errors,
            logger,
            time,
            stoppingToken);
}
