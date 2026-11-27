namespace MB.ScopedDataSync.Messaging.Kafka.DependencyInjection;

public sealed class ScopedDataSyncKafkaOptions
{
    public string? BootstrapServers { get; set; }

    public string? ClientId { get; set; }

    /// <summary>
    /// Grupo do serviço. Vale para todos os tópicos registrados com <c>FromKafkaTopic</c>.
    /// </summary>
    public string? GroupId { get; set; }
}
