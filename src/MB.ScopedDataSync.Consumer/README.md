# MB.ScopedDataSync.Consumer

Composição do consumidor: no máximo um cache e no máximo um store de banco, cerca de `Version`, staleness e cooldown. O domínio enxerga `IScopeReader<T>`.

Referenciar este pacote traz `MB.ScopedDataSync.Abstractions` de forma transitiva.

## Quando usar

No `Program.cs` do serviço que aplica eventos de entidade e serve leitura para o domínio.

## Quando não usar

Não use este pacote para alocar `Version` nem escrever na outbox. Não chame `UseInMemoryCache` e `UseRedisCache` no mesmo builder: a segunda chamada lança, porque não existe cadeia InMemory, Redis e banco.

## Instalação

```bash
dotnet add package MB.ScopedDataSync.Consumer
dotnet add package MB.ScopedDataSync.Caching.InMemory
```

`UseInMemoryCache`, `UseRedisCache` e `UsePostgresStore` vivem nos adaptadores. O builder oferece os pontos `UseCacheProvider` e `UseDatabaseStore`.

## Configuração

```csharp
services.AddSingleton<IScopedDataSyncErrorHandler, OpsSyncErrorHandler>();
services.AddSingleton<IStaleAlertSink, OpsStaleAlertSink>();
services.AddScopedDataSync<PositionSnapshot>(builder =>
{
    builder.WithScope("positions");
    builder.WithStaleness(options =>
    {
        options.StaleAfter = TimeSpan.FromMinutes(2);
        options.AlertCooldown = TimeSpan.FromMinutes(10);
    });
    builder.UseInMemoryCache(cache => cache.PayloadTtl = TimeSpan.FromMinutes(20));
});
services.AddScopedDataSync<EntityProfile>(builder =>
{
    builder.WithScope("profile");
    builder.WithStaleness(options =>
    {
        options.StaleAfter = TimeSpan.FromMinutes(2);
        options.AlertCooldown = TimeSpan.FromMinutes(10);
    });
    builder.UseInMemoryCache(cache => cache.PayloadTtl = TimeSpan.FromMinutes(20));
});
```

Sem cache e sem store de banco, o registro falha na composição. O mesmo `T` não pode ser registrado duas vezes. Escopos diferentes usam tipos de payload diferentes.

## Leitura e evento

```csharp
public sealed class PortfolioService(IScopeReader<PositionSnapshot> reader)
{
    public Task<ReadResult<PositionSnapshot>> GetAsync(string entityId, CancellationToken cancellationToken)
        => reader.ReadAsync(entityId, cancellationToken);
}

var updater = services.BuildServiceProvider().GetRequiredEventConsumerUpdater<PositionSnapshot>();
await updater.ApplyEventAsync(
    new ScopedEventEnvelope<PositionSnapshot>("inv-1", "positions", version, snapshot, occurredAt),
    cancellationToken);
```

`GetService<IProjectionUpdater<T>>()` sem a chave `ScopedDataSyncServiceKeys.EventConsumer` devolve null. Versão menor ou igual é `DiscardedStaleOrDuplicate`. Quando há store de banco, a recusa do upsert condicional impede o cache de avançar.

## Kafka, Redis e a leitura da aplicação

Este pacote não abre cliente Kafka. Quem quiser o broker instala `MB.ScopedDataSync.Messaging.Kafka` e registra `FromKafkaTopic` junto do escopo. `AddScopedDataSyncKafka` sobe o hosted service, aplica o evento e grava o cache ou o banco configurados aqui. Sem esse pacote, um arquivo ou um job chama `GetRequiredEventConsumerUpdater<T>()` por conta própria.

O domínio só chama `IScopeReader<T>`. Payload ainda no cache volta do cache e não consulta o banco. Sem evento ainda, `Data` vem nulo e `Freshness.Status` é `Missing`.

```csharp
services.AddScopedDataSyncKafka(kafka =>
{
    kafka.BootstrapServers = configuration["Kafka:BootstrapServers"]!;
    kafka.ClientId = configuration["Kafka:ClientId"]!;
    kafka.GroupId = configuration["Kafka:GroupId"]!;
});
services.AddScopedDataSync<PositionSnapshot>(builder =>
{
    builder.WithScope("positions");
    builder.FromKafkaTopic("entity.positions");
    builder.UseRedisCache(BindRedis);
});
services.AddScopedDataSync<EntityProfile>(builder =>
{
    builder.WithScope("profile");
    builder.FromKafkaTopic("entity.profile");
    builder.UseRedisCache(BindRedis);
});

void BindRedis(RedisCacheOptions cache)
{
    cache.ConnectionString = configuration["Redis:ConnectionString"];
    cache.ServiceName = configuration["Redis:ServiceName"]!;
    cache.Database = configuration.GetValue("Redis:Database", -1);
    var payloadTtl = configuration["Redis:PayloadTtl"];
    if (!string.IsNullOrWhiteSpace(payloadTtl))
    {
        cache.PayloadTtl = TimeSpan.Parse(payloadTtl);
    }
}
```

`GroupId` é um por serviço e cobre os dois tópicos. A ordem das duas chamadas não importa. O cliente, o offset e o `appsettings` estão no README de `MB.ScopedDataSync.Messaging.Kafka`.

A aplicação não vê tópico nem Redis. `ReadAsync` devolve o que o apply gravou:

```csharp
public sealed class PortfolioService(IScopeReader<PositionSnapshot> reader)
{
    public async Task<PositionSnapshot?> GetAsync(string entityId, CancellationToken cancellationToken)
    {
        var result = await reader.ReadAsync(entityId, cancellationToken);
        return result.Data;
    }
}
```

## Falha contida

`IScopedDataSyncErrorHandler` é opcional. Registre antes de `AddScopedDataSync`. Sem essa linha, a lib loga e segue.

Falha do store principal em `ReadAsync`, `ApplyEventAsync` e `RecordSyncFailureAsync` continua subindo para o serviço. O handler não é chamado. O mesmo vale para `PublishAsync` do produtor e para um `DispatchAsync` chamado direto. O hosted service do relay contém essa falha e avisa `OutboxRelayFaulted`.

Com cache e banco, falha de leitura do cache chega como `CacheDegraded` e `ReadAsync` devolve o que está no banco. Falha do sink de staleness chega como `StaleAlertFailed` e a leitura devolve o payload. Uma exceção do handler é logada e não derruba a operação. O handler não troca o resultado da leitura.

`KafkaMessageRetrying` e `KafkaConsumerFaulted` aparecem quando o pacote `MB.ScopedDataSync.Messaging.Kafka` está no host. O offset e o loop estão no README desse pacote. `OutboxRelayFaulted` aparece no serviço que publica, quando `AddScopedDataSyncOutboxRelay` está no host.

```csharp
services.AddSingleton<IScopedDataSyncErrorHandler, OpsSyncErrorHandler>();
services.AddSingleton<IStaleAlertSink, OpsStaleAlertSink>();

services.AddScopedDataSyncKafka(kafka =>
{
    kafka.BootstrapServers = configuration["Kafka:BootstrapServers"]!;
    kafka.ClientId = configuration["Kafka:ClientId"]!;
    kafka.GroupId = configuration["Kafka:GroupId"]!;
});

services.AddScopedDataSync<PositionSnapshot>(sync =>
{
    sync.WithScope("positions");
    sync.FromKafkaTopic("entity.positions");
    sync.UseRedisCache(BindRedis);
    sync.UsePostgresStore(store =>
        store.ConnectionString = configuration.GetConnectionString("EntitySync")!);
});

public sealed class OpsSyncErrorHandler(ILogger<OpsSyncErrorHandler> logger) : IScopedDataSyncErrorHandler
{
    public Task HandleAsync(ScopedDataSyncFailure failure, CancellationToken cancellationToken = default)
    {
        switch (failure.Kind)
        {
            case ScopedDataSyncFailureKind.CacheDegraded:
                logger.LogWarning(
                    failure.Exception,
                    "Leitura de {Scope} da entidade {EntityId} caiu para o banco.",
                    failure.Scope,
                    failure.EntityId);
                break;

            case ScopedDataSyncFailureKind.StaleAlertFailed:
                logger.LogWarning(
                    failure.Exception,
                    "Alerta de {Scope} da entidade {EntityId} não foi publicado.",
                    failure.Scope,
                    failure.EntityId);
                break;

            case ScopedDataSyncFailureKind.KafkaMessageRetrying:
                logger.LogError(
                    failure.Exception,
                    "Evento de {Scope} da entidade {EntityId} permanece na partição.",
                    failure.Scope,
                    failure.EntityId);
                break;

            case ScopedDataSyncFailureKind.KafkaConsumerFaulted:
                logger.LogError(
                    failure.Exception,
                    "Consumidor Kafka falhou em {Operation} e o loop continua.",
                    failure.Operation);
                break;
        }

        return Task.CompletedTask;
    }
}
```

`failure.Operation` diz o passo: `entity.projection.read`, `entity.projection.stale_alert`, `kafka.apply`, `kafka.record_failure`, `kafka.consume`, `kafka.commit`, `kafka.resume`, `kafka.dispatch` ou `outbox.dispatch`. `failure.Exception` é a falha original.

A leitura do serviço não muda. Com Redis e Postgres, uma queda do cache chega no handler como `CacheDegraded` e `GetAsync` devolve o banco:

```csharp
public sealed class PortfolioService(IScopeReader<PositionSnapshot> reader)
{
    public async Task<PositionSnapshot?> GetAsync(string entityId, CancellationToken cancellationToken)
    {
        var result = await reader.ReadAsync(entityId, cancellationToken);
        return result.Data;
    }
}
```

Se o Postgres cair nessa leitura, ou se só existir cache e o Redis cair, `ReadAsync` continua lançando. O job que aplica o envelope por conta própria trata essa exceção no próprio `catch`:

```csharp
try
{
    await updater.ApplyEventAsync(envelope, cancellationToken);
}
catch (Exception exception) when (exception is not OperationCanceledException)
{
    logger.LogError(exception, "Apply de {EntityId} falhou.", envelope.EntityId);
    throw;
}
```

## Staleness

A idade é `agora - OccurredAtUtc` do evento aplicado. A partir de `StaleAfter`, a leitura continua devolvendo o payload e consulta `IAlertCooldownGate` na chave `scope:entityId`. Um único vencedor publica `StaleRecordAlert`. Dentro de `AlertCooldown`, `FreshnessInfo.AlertSuppressedByCooldown` fica verdadeiro. Falha consecutiva com dado ainda fresco produz `SyncImpaired` e não dispara esse alerta.

## Store de banco

`UseDatabaseStore` aceita no máximo um banco, relacional ou NoSQL, sozinho ou ao lado do cache. `UsePostgresStore` é a implementação PostgreSQL. O SQL condicional fica encapsulado em `MB.ScopedDataSync.Storage.Postgres`; não há API de upsert livre para o domínio. O tópico compactado, a chave `entityId` e o uso de `[]` no lugar de tombstone estão no `MB.ScopedDataSync.Producer` e no README da solution.

O host lê o próprio `appsettings`. A projeção usa `ConnectionStrings:EntitySync`. O cache usa a seção `Redis` (`ConnectionString`, `ServiceName`, `Database`, `PayloadTtl`). O consumidor Kafka opcional está na seção anterior e no pacote `MB.ScopedDataSync.Messaging.Kafka`. O arquivo de `appsettings` com PostgreSQL está no README da solution.
