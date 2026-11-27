namespace MB.ScopedDataSync.Producer.DependencyInjection;

/// <summary>
/// Ritmo do hosted service que chama <c>PostgresOutboxRelay.DispatchAsync</c>.
/// Lote com linhas volta na hora. Lote vazio espera <see cref="IdleDelay"/>.
/// </summary>
public sealed class OutboxRelayOptions
{
    public int BatchSize { get; set; } = 100;

    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(1);
}
