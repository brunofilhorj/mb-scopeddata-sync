namespace MB.ScopedDataSync.Consumer.Staleness;

/// <summary>
/// Controla o cooldown anti-ruído por chave de alerta. <see cref="TryEnter"/> é atômico:
/// chamadas concorrentes para a mesma chave produzem um único vencedor.
/// </summary>
public interface IAlertCooldownGate
{
    bool TryEnter(string key, TimeSpan cooldown, DateTimeOffset now);

    void Undo(string key, DateTimeOffset enteredAt);
}
