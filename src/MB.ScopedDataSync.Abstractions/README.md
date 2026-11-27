# MB.ScopedDataSync.Abstractions

Contratos da sincronização de projeções de entidade: leitura com frescor, atualização exclusiva do consumidor de eventos, alerta de staleness, falha contida e envelope versionado.

## Quando usar

Referencie este pacote direto apenas em um projeto que precisa dos tipos e não hospeda o builder, o cache nem o produtor. `Consumer`, `Caching.InMemory`, `Caching.Redis`, `Storage.Postgres`, `Observability` e `Producer` já dependem dele sem `PrivateAssets`, então o contrato chega de forma transitiva.

## Quando não usar

Não implemente persistência aqui e não registre DI por este pacote. Também não trate `IProjectionUpdater<T>` como dependência de domínio: a interface está marcada com `EventConsumerOnly`.

## Instalação

```bash
dotnet add package MB.ScopedDataSync.Abstractions
```

Na prática, instale `MB.ScopedDataSync.Consumer` ou o adaptador. Os tipos abaixo ficam disponíveis sem este `PackageReference`.

## Contratos

`IScopeReader<T>.ReadAsync` devolve `ReadResult<T>` com `FreshnessInfo` e `DataFreshnessStatus` (`Missing`, `Fresh`, `Stale`, `SyncImpaired`).

`IProjectionUpdater<T>` expõe `ApplyEventAsync` e `RecordSyncFailureAsync`. O apply só persiste quando `incoming.Version` é maior que a versão armazenada. A falha não reduz `Version`.

`ScopedEventEnvelope<TPayload>` carrega `EntityId`, `Scope`, `Version`, payload e `OccurredAtUtc`. `IStaleAlertSink` recebe `StaleRecordAlert` quando o consumidor decide que o cooldown permitiu o alerta.

`IScopedDataSyncErrorHandler` recebe só a falha que a biblioteca já conteve. Sem registro, o log segue sozinho. Falha do store principal em `ReadAsync`, `ApplyEventAsync`, `RecordSyncFailureAsync` e em `PublishAsync` continua subindo para quem chamou. O loop do relay contém a falha de `DispatchAsync` e avisa `OutboxRelayFaulted`. Uma chamada direta de `DispatchAsync` continua lançando. O handler não confirma offset e não troca o resultado da leitura. Se o método lançar, a exceção é logada e o comportamento padrão permanece.

| `ScopedDataSyncFailureKind` | Quando chega |
| --- | --- |
| `CacheDegraded` | A leitura do cache falhou e existe banco. A leitura segue no banco. `Operation` é `entity.projection.read` |
| `StaleAlertFailed` | O sink de staleness falhou. O cooldown foi desfeito e o payload volta na leitura. `Operation` é `entity.projection.stale_alert` |
| `KafkaMessageRetrying` | O consumidor Kafka não gravou. O offset da partição fica aberto. `Operation` é `kafka.apply` ou `kafka.record_failure` |
| `KafkaConsumerFaulted` | `Consume`, `Commit`, retomada da partição ou o despacho falhou. O loop continua. `Operation` é `kafka.consume`, `kafka.commit`, `kafka.resume` ou `kafka.dispatch` |
| `OutboxRelayFaulted` | O relay da outbox falhou ao reservar ou publicar um lote. A linha segue sem `published_at_utc` e o loop continua. `Operation` é `outbox.dispatch` |

`IProjectionStore<T>` é o ponto de extensão de cache e de store de banco, relacional ou NoSQL. `TryApplyAsync` é o passo atômico da cerca de versão.

## Exemplo de leitura

```csharp
public sealed class SuitabilityService(IScopeReader<SuitabilitySnapshot> reader)
{
    public Task<ReadResult<SuitabilitySnapshot>> GetAsync(string entityId, CancellationToken cancellationToken)
        => reader.ReadAsync(entityId, cancellationToken);
}
```

O registro do handler fica antes de `AddScopedDataSync`. O exemplo completo está no README de `MB.ScopedDataSync.Consumer` e no de `MB.ScopedDataSync.Messaging.Kafka`.

```csharp
services.AddSingleton<IScopedDataSyncErrorHandler, OpsSyncErrorHandler>();
services.AddScopedDataSync<PositionSnapshot>(sync =>
{
    sync.WithScope("positions");
    sync.UseRedisCache(BindRedis);
    sync.UsePostgresStore(store =>
        store.ConnectionString = configuration.GetConnectionString("EntitySync")!);
});
```

A configuração do builder, o cooldown e o tópico Kafka estão no README do repositório e nos pacotes `Consumer` e `Producer`.
