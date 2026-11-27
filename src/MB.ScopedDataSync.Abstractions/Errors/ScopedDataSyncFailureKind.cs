namespace MB.ScopedDataSync.Abstractions.Errors;

/// <summary>
/// Classifica a falha que a biblioteca já conteve. Quem chamou o método principal não recebe estas falhas como exceção.
/// </summary>
public enum ScopedDataSyncFailureKind
{
    /// <summary>A leitura do cache falhou e o banco respondeu no lugar.</summary>
    CacheDegraded = 0,

    /// <summary>O sink de staleness falhou. O cooldown foi desfeito e a leitura segue com o payload.</summary>
    StaleAlertFailed = 1,

    /// <summary>O consumidor Kafka não gravou o evento. O offset dessa partição permanece aberto.</summary>
    KafkaMessageRetrying = 2,

    /// <summary>Consume ou commit do Kafka falhou. O loop do consumidor continua.</summary>
    KafkaConsumerFaulted = 3,

    /// <summary>O relay da outbox falhou ao reservar ou publicar um lote. O loop continua e a linha segue sem <c>published_at_utc</c>.</summary>
    OutboxRelayFaulted = 4
}
