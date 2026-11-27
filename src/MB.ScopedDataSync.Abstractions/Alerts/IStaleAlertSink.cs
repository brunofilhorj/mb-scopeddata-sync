namespace MB.ScopedDataSync.Abstractions.Alerts;

/// <summary>
/// Destino dos alertas de staleness. O consumidor só publica quando o cooldown da chave expirou.
/// </summary>
public interface IStaleAlertSink
{
    Task PublishAsync(StaleRecordAlert alert, CancellationToken cancellationToken = default);
}
