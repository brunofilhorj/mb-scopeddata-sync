namespace MB.ScopedDataSync.Consumer.Staleness;

/// <summary>
/// Política de frescor. <see cref="DataFreshnessStatus.Stale"/> quando a idade do payload
/// atinge <see cref="StalenessOptions.StaleAfter"/>. Falha de sync com payload ainda dentro
/// da janela produz <see cref="DataFreshnessStatus.SyncImpaired"/> e não dispara alerta de staleness.
/// Sem payload o status é <see cref="DataFreshnessStatus.Missing"/>, mesmo que exista watermark de versão.
/// </summary>
public static class StalenessEvaluator
{
    public static FreshnessInfo Evaluate<TPayload>(
        ProjectionRecord<TPayload>? record,
        StalenessOptions options,
        DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (record is null || !record.HasPayload || record.UpdatedAtUtc is null)
        {
            return new FreshnessInfo(
                DataFreshnessStatus.Missing,
                record?.Version > 0 ? record.Version : null,
                null,
                null,
                record?.ConsecutiveFailures ?? 0,
                AlertSuppressedByCooldown: false);
        }

        var age = now - record.UpdatedAtUtc.Value;
        if (age < TimeSpan.Zero)
        {
            age = TimeSpan.Zero;
        }

        var status = age >= options.StaleAfter
            ? DataFreshnessStatus.Stale
            : record.ConsecutiveFailures > 0
                ? DataFreshnessStatus.SyncImpaired
                : DataFreshnessStatus.Fresh;

        return new FreshnessInfo(
            status,
            record.Version,
            record.UpdatedAtUtc,
            age,
            record.ConsecutiveFailures,
            AlertSuppressedByCooldown: false);
    }
}
