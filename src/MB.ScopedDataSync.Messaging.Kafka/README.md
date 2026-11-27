# MB.ScopedDataSync.Messaging.Kafka

Kafka opcional dos dois lados. No consumidor, um hosted service assina um tópico por escopo e chama `ApplyEventAsync`. O domínio continua lendo por `IScopeReader<T>`. No produtor, `AddScopedDataSyncKafkaProducer` registra o cliente da outbox com `acks=all`.

Sem este pacote, o serviço aplica o envelope do próprio jeito: arquivo, job, outro broker. `AddScopedDataSync<T>` não abre cliente Kafka. A outbox continua em `MB.ScopedDataSync.Producer`, e um transporte próprio implementa `IOutboxKafkaPublisher`.

Instalar este pacote traz `Consumer`, `Producer` e, de forma transitiva, `Abstractions`.

## Quando usar

O fato chega por evento no tópico compactado e o serviço quer manter cache ou banco atualizados sem escrever o `BackgroundService`. No serviço que é a fonte, o mesmo pacote envia a linha da outbox sem um cliente Kafka escrito à mão.

## Quando não usar

Não use `AddScopedDataSyncKafkaProducer` se o transporte não for Kafka: implemente `IOutboxKafkaPublisher` no serviço. Não use o consumidor se a projeção entra por outro canal: nesse caso chame `GetRequiredEventConsumerUpdater<T>()` no job do serviço.

## Instalação

```bash
dotnet add package MB.ScopedDataSync.Messaging.Kafka
dotnet add package MB.ScopedDataSync.Caching.Redis
```

## Bootstrap

A ordem entre `AddScopedDataSyncKafka` e `AddScopedDataSync<T>` não importa. O host lê os tópicos na subida. `FromKafkaTopic` sem o cliente, ou o cliente sem nenhum tópico, falha nessa subida, antes de assinar o broker.

```csharp
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
    sync.UseRedisCache(cache =>
    {
        cache.ConnectionString = configuration["Redis:ConnectionString"];
        cache.ServiceName = configuration["Redis:ServiceName"]!;
        cache.Database = configuration.GetValue("Redis:Database", -1);
    });
});

services.AddScopedDataSync<EntityProfile>(sync =>
{
    sync.WithScope("profile");
    sync.FromKafkaTopic("entity.profile");
    sync.UseRedisCache(cache =>
    {
        cache.ConnectionString = configuration["Redis:ConnectionString"];
        cache.ServiceName = configuration["Redis:ServiceName"]!;
        cache.Database = configuration.GetValue("Redis:Database", -1);
    });
});
```

```json
{
  "Kafka": {
    "BootstrapServers": "kafka-1.internal:9092,kafka-2.internal:9092",
    "ClientId": "positions-api",
    "GroupId": "positions-api"
  }
}
```

`GroupId` é um por serviço e cobre todos os `FromKafkaTopic`. O nome do tópico é o gravado na outbox daquele escopo. A chave da mensagem é o `entityId`. O valor traz `entityId`, `scope`, `version`, `occurredAtUtc` e `payload`.

## Produtor

O serviço de negócio chama `IScopedEventPublisher.PublishAsync`. Este pacote só entra no envio: registra o cliente e deixa o relay do `Producer` publicar.

`BootstrapServers` ou `ClientId` vazios falham na composição. Uma segunda chamada também. Um `IOutboxKafkaPublisher` já registrado impede este método. Um publisher acrescentado depois falha na subida.

```csharp
services.AddScopedDataSyncProducer(options =>
{
    options.ConnectionString = configuration.GetConnectionString("EntityOutbox");
});
services.AddScopedDataSyncKafkaProducer(kafka =>
{
    kafka.BootstrapServers = configuration["Kafka:BootstrapServers"]!;
    kafka.ClientId = configuration["Kafka:ClientId"]!;
    kafka.Configure = config =>
    {
        config.SecurityProtocol = SecurityProtocol.SaslSsl;
        config.SaslUsername = configuration["Kafka:Username"];
        config.SaslPassword = configuration["Kafka:Password"];
    };
});
services.AddScopedDataSyncOutboxRelay(relay =>
{
    relay.BatchSize = 100;
    relay.IdleDelay = TimeSpan.FromSeconds(1);
});
```

A chave da mensagem é `OutboxKafkaMessage.Key`. O valor é o JSON já montado, e `[]` permanece `[]`. `Configure` ajusta a segurança do cluster. `Acks` volta para `All` depois do callback. O ritmo do lote e a falha `OutboxRelayFaulted` estão no README de `MB.ScopedDataSync.Producer`.

## Offset

Auto-commit fica desligado.

- Apply aceito, ou descarte por versão antiga, confirma o offset.
- Falha ao gravar cache ou banco não confirma. A partição dessa mensagem pausa, o handler recebe `KafkaMessageRetrying` e as outras partições seguem. Depois de 1 segundo a mesma mensagem é lida de novo.
- Falha de `Consume` ou `Commit` não derruba o processo. O handler recebe `KafkaConsumerFaulted` e o loop continua. Commit que falha também deixa o offset aberto.
- Payload inválido, escopo diferente do registrado ou versão que não é positiva chama `RecordSyncFailureAsync` quando o `entityId` é legível, e confirma o offset. Um registro quebrado não trava a partição e não chama o handler. Se a falha nem chega a ser gravada, o offset fica aberto.

## Falha contida

O mesmo `IScopedDataSyncErrorHandler` do `Consumer` cobre o loop. Registre antes de `AddScopedDataSync`. Sem registro, a lib loga e segue. O handler não confirma offset e não impede o retry. Se ele lançar, a exceção é logada e o loop continua.

Quando o apply entra pelo hosted service, o serviço não tem `try` em volta da mensagem. A falha de gravar vira `KafkaMessageRetrying`. `KafkaConsumerFaulted` é o broker, o `Consume` ou o `Commit`.

```csharp
services.AddSingleton<IScopedDataSyncErrorHandler, OpsSyncErrorHandler>();

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

            case ScopedDataSyncFailureKind.OutboxRelayFaulted:
                logger.LogError(
                    failure.Exception,
                    "Relay da outbox falhou e o lote continua sem published_at_utc.");
                break;
        }

        return Task.CompletedTask;
    }
}
```

`failure.Operation` é `kafka.apply`, `kafka.record_failure`, `kafka.consume`, `kafka.commit`, `kafka.resume`, `kafka.dispatch` ou `outbox.dispatch`. `CacheDegraded` e `StaleAlertFailed` também chegam neste handler quando o escopo tem cache, banco ou sink de staleness. `OutboxRelayFaulted` chega no serviço que publica. O `catch` do job que chama `ApplyEventAsync` direto continua sendo do serviço: essa falha não passa pelo handler.

## Leitura

O domínio não vê tópico nem cliente. `ReadAsync` devolve o snapshot gravado pelo apply. Sem evento ainda, `Data` vem nulo e `Freshness.Status` é `Missing`.
