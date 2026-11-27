namespace MB.ScopedDataSync.Abstractions.Errors;

/// <summary>
/// Aviso opcional das falhas que a biblioteca contém.
/// Sem registro, o consumidor loga e segue. Falha do store principal continua subindo para quem chamou.
/// Uma exceção deste método é logada e não altera o comportamento padrão.
/// </summary>
public interface IScopedDataSyncErrorHandler
{
    Task HandleAsync(ScopedDataSyncFailure failure, CancellationToken cancellationToken = default);
}
