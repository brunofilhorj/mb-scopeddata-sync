# AGENTS.md

Regras para alterar `MB.ScopedDataSync`, bibliotecas .NET 10 empacotadas no NuGet interno da MB.

## Pacotes

| Projeto | Pacote | Papel |
| --- | --- | --- |
| `src/MB.ScopedDataSync.Abstractions` | `MB.ScopedDataSync.Abstractions` | Contratos de leitura, atualização, alerta, envelope e store |
| `src/MB.ScopedDataSync.Consumer` | `MB.ScopedDataSync.Consumer` | Fluent builder, versão, staleness, cooldown e fronteira do domínio |
| `src/MB.ScopedDataSync.Caching.InMemory` | `MB.ScopedDataSync.Caching.InMemory` | `IMemoryCache`, watermark de `Version` e TTL do payload |
| `src/MB.ScopedDataSync.Caching.Redis` | `MB.ScopedDataSync.Caching.Redis` | Hash tag `{entityId}` e scripts Lua atômicos |
| `src/MB.ScopedDataSync.Storage.Postgres` | `MB.ScopedDataSync.Storage.Postgres` | Store PostgreSQL da projeção, com upsert condicional |
| `src/MB.ScopedDataSync.Observability` | `MB.ScopedDataSync.Observability` | `Meter` e `ActivitySource` opcionais; dashboard Grafana e regras Prometheus no pacote, sem exporter |
| `src/MB.ScopedDataSync.Producer` | `MB.ScopedDataSync.Producer` | `Version` monotônica e outbox na mesma transação |
| `src/MB.ScopedDataSync.Messaging.Kafka` | `MB.ScopedDataSync.Messaging.Kafka` | Hosted service opcional que assina um tópico por escopo e aplica o envelope |

`Abstractions` é referência de projeto sem `PrivateAssets`. Quem instala `Consumer`, um adaptador ou `Producer` recebe os contratos de forma transitiva.

Projetos em `tests/`, `samples/` e `integration/` mantêm `IsPackable=false` e `GeneratePackageOnBuild=false`. `samples/` e `integration/` ficam fora de `src` e da suíte unitária.

## Composição

`AddScopedDataSync<T>` aceita no máximo um cache (`UseInMemoryCache` ou `UseRedisCache`) e no máximo um store de banco (`UseDatabaseStore`; hoje, `UsePostgresStore`). O banco pode existir sozinho ou ao lado do cache escolhido. Não introduz uma cadeia InMemory, Redis e banco.

Leitura com hit de cache não consulta o banco. Miss, ou payload expirado, lê o store de banco e reidrata o cache pela mesma cerca de versão. Se o upsert do banco recusar o evento, o cache não avança.

O domínio recebe `IScopeReader<T>`. `IProjectionUpdater<T>` fica registrado apenas com a chave `ScopedDataSyncServiceKeys.EventConsumer` e marcado com `EventConsumerOnly`. `MB.ScopedDataSync.Messaging.Kafka` é o adaptador opcional que abre o consumidor: `FromKafkaTopic` liga o escopo ao tópico e `AddScopedDataSyncKafka` sobe o hosted service. Sem esse pacote, o serviço aplica o envelope com o próprio job.

## Version, staleness e outbox

- Aplicar evento só quando `incoming.Version > current.Version`. Igualdade é descarte idempotente.
- A comparação e a gravação acontecem no mesmo passo atômico: lock em memória, script Lua ou `ON CONFLICT ... WHERE tabela.version < EXCLUDED.version`.
- A watermark de versão sobrevive ao TTL do payload.
- `RecordSyncFailureAsync` incrementa falhas e não altera `Version` nem o payload aceito.
- Staleness usa a idade do payload contra `StaleAfter`. O alerta passa por `IAlertCooldownGate` na chave `scope:entityId`. Falha do sink devolve o cooldown e não falha a leitura.
- O produtor incrementa `entity_scope_versions` e insere `outbox_events` antes do commit. `kafka_key` é o `entityId`.
- Coleção vazia ou nula é serializada como `[]`. Payload JSON `null` é tombstone de tópico compactado e não é publicado.

## Código e testes

.NET 10, C# atual, nullable e warnings como erro vêm de `Directory.Build.props`. Metadados NuGet base (`Company`, `Authors`, `PackageId`, `VersionPrefix`) ficam nesse arquivo.

Comentários apenas em contratos públicos, na ordenação por `Version`, na política de staleness e cooldown, na atomicidade e nos pontos de extensão (`CacheProviderRegistration<T>`, `DatabaseStoreRegistration<T>`, scripts e SQL).

Testes xUnit em `tests/MB.ScopedDataSync.Tests`: ordenação e idempotência, builder com um cache e store de banco opcional, cooldown, restrição de acesso do domínio, geração da outbox e a decisão de offset do consumidor Kafka. SQL e Lua são o contrato dos passos atômicos que não sobem banco nem Redis no teste unitário. O teste do consumidor Kafka não abre broker.

`samples/MB.ScopedDataSync.InMemoryExample` mostra o producer e o consumer ligados por `OutboxKafkaMessage`, com outbox e cache em memória. `samples/MB.ScopedDataSync.ObservabilityLocal` sobe um host com `/metrics`, Prometheus e Grafana para o dashboard do pacote. `integration/MB.ScopedDataSync.Containers` executa o SQL e o Lua via Testcontainers e pula o teste quando `docker info` falha.

`dotnet test MB.ScopedDataSync.slnx` é a verificação mínima antes de empacotar.
