# MB.ScopedDataSync.Producer

Lado produtor: incrementa `Version` por `(entityId, scope)` e grava `outbox_events` na mesma transação PostgreSQL. A chave Kafka é `entityId`. Coleção vazia sai como `[]`.

`MB.ScopedDataSync.Abstractions` chega de forma transitiva, para as regras de nome e o JSON compartilhado. O serviço consumidor usa `MB.ScopedDataSync.Consumer`, não este pacote, para aplicar a projeção.

## Quando usar

No serviço que é a fonte do fato e precisa publicar o escopo da entidade com versão monotônica e outbox.

## Quando não usar

Não use para ler projeção nem para upsert da tabela do consumidor. Não publique `null`: em tópico compactado isso é tombstone e apaga a chave.

## Instalação

```bash
dotnet add package MB.ScopedDataSync.Producer
dotnet add package MB.ScopedDataSync.Messaging.Kafka
```

```csharp
services.AddScopedDataSyncProducer(options =>
{
    options.ConnectionString = configuration.GetConnectionString("EntityOutbox");
});
services.AddScopedDataSyncKafkaProducer(kafka =>
{
    kafka.BootstrapServers = configuration["Kafka:BootstrapServers"]!;
    kafka.ClientId = configuration["Kafka:ClientId"]!;
});
services.AddScopedDataSyncOutboxRelay(relay =>
{
    relay.BatchSize = 100;
    relay.IdleDelay = TimeSpan.FromSeconds(1);
});
```

`AddScopedDataSyncKafkaProducer` fica em `MB.ScopedDataSync.Messaging.Kafka` e registra `IOutboxKafkaPublisher`. `AddScopedDataSyncOutboxRelay` sobe o hosted service que chama `DispatchAsync`. Lote com linhas volta na hora. Lote vazio espera `IdleDelay`, no padrão 1 segundo, em lotes de 100. Falha de reserva ou de envio não marca `published_at_utc`, avisa `OutboxRelayFaulted` e o loop continua. Uma segunda chamada do relay lança.

Um transporte que não seja Kafka implementa `IOutboxKafkaPublisher` e não chama `AddScopedDataSyncKafkaProducer`. Registrar os dois falha na composição. Um publisher acrescentado depois falha na subida do host.

## appsettings

```json
{
  "ConnectionStrings": {
    "EntityOutbox": "Host=postgres.internal;Port=5432;Database=entity;Username=outbox;Password=change-me;SSL Mode=Prefer"
  },
  "Kafka": {
    "BootstrapServers": "kafka-1.internal:9092,kafka-2.internal:9092",
    "ClientId": "positions-producer",
    "Topics": {
      "positions": "entity.positions",
      "profile": "entity.profile"
    }
  }
}
```

`GetConnectionString("EntityOutbox")` é o PostgreSQL de `entity_scope_versions` e `outbox_events`, no formato do Npgsql. O consumidor da projeção usa `ConnectionStrings:EntitySync`, mesmo que o servidor coincida.

`AddScopedDataSyncKafkaProducer` lê `Kafka:BootstrapServers` e `Kafka:ClientId`, e produz com `Acks=All`. O callback `Configure` ajusta SASL ou SSL. `Acks` volta para `All` depois dele. O mapa `Kafka:Topics` é configuração do host: a chave é o escopo e o valor é o tópico. Cada `PublishAsync` recebe o tópico daquele escopo no terceiro argumento de `PublishEntityEventRequest`. O mesmo `AddScopedDataSyncProducer` e a mesma connection string servem todos os escopos. Uma publicação não se replica para vários tópicos: cada chamada incrementa a versão daquele escopo e gera uma linha de outbox. A chave da mensagem é `OutboxKafkaMessage.Key` (`entityId`). `cleanup.policy` e `retention.ms` configuram cada tópico no broker, fora deste arquivo.

`EnsureSchemaOnStartup` cria `entity_scope_versions` e `outbox_events`. Há índice único `(entity_id, scope, version)` e índice parcial das linhas ainda não publicadas. Passe `false` quando a migração do serviço já criou as tabelas.

## Publicação

```csharp
var publisher = provider.GetRequiredService<IScopedEventPublisher>();
var positions = await publisher.PublishAsync(
    new PublishEntityEventRequest<IReadOnlyList<PositionSnapshot>>(
        entityId,
        "positions",
        configuration["Kafka:Topics:positions"]!,
        remainingPositions));
var profile = await publisher.PublishAsync(
    new PublishEntityEventRequest<EntityProfile>(
        entityId,
        "profile",
        configuration["Kafka:Topics:profile"]!,
        entityProfile));

var positionsMessage = OutboxKafkaMessage.Create(positions);
var profileMessage = OutboxKafkaMessage.Create(profile);
```

A transação executa, nesta ordem:

```sql
INSERT INTO entity_scope_versions (entity_id, scope, version)
VALUES (@entity_id, @scope, 1)
ON CONFLICT (entity_id, scope)
DO UPDATE SET version = entity_scope_versions.version + 1
RETURNING version;
```

Em seguida insere a outbox com `kafka_key = entityId` e o payload JSON. Falha na inserção desfaz o incremento. `PublishAsync` devolve a `Version` e não fala com o broker. O hosted service reserva com `FOR UPDATE SKIP LOCKED`, chama `IOutboxKafkaPublisher` e só então marca `published_at_utc`. `DispatchAsync` chamado direto ainda lança para quem chamou.

Lista vazia, lista nula e remoção do último item serializam `[]`. Um objeto de payload nulo lança, em vez de virar tombstone.

O consumidor aplica com `incoming.Version > current.Version`. Reentrega da mesma versão é descarte idempotente. Staleness e cooldown ficam no `Consumer`. O store de banco do consumidor, quando configurado, usa o upsert condicional do `Storage.Postgres` e não esta tabela de versões.

## Tópico por escopo

Crie um tópico por escopo (`entity.positions`, `entity.profile`). Produza com a chave `OutboxKafkaMessage.Key`, que é o `entityId`, para preservar a ordem na partição.

```properties
cleanup.policy=compact
retention.ms=-1
min.cleanable.dirty.ratio=0.1
segment.ms=300000
delete.retention.ms=86400000
min.insync.replicas=2
```

`cleanup.policy=compact` retém a última mensagem de cada chave. `retention.ms=-1` não expira por tempo. `min.cleanable.dirty.ratio=0.1` e `segment.ms=300000` permitem compactar sem esperar o segmento padrão de sete dias. `delete.retention.ms` reteria tombstones; este pacote não os emite. `[]` mantém a chave da entidade com uma coleção vazia. `acks=all` no cliente. Não traduza o campo `payload` de `[]` para `null`.
