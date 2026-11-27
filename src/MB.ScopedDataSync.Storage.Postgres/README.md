# MB.ScopedDataSync.Storage.Postgres

Store PostgreSQL da projeção, via `NpgsqlDataSource`. Pode ser o único store ou acompanhar um cache. O upsert é condicional e o SQL permanece dentro do pacote.

Instalar este pacote traz `Consumer` e, de forma transitiva, `Abstractions`.

## Quando usar

A projeção precisa de um store de banco, sozinho ou para reidratar o cache quando o payload expira.

## Quando não usar

Não use este pacote para numerar eventos novos. A sequência de negócio nasce em `MB.ScopedDataSync.Producer`. Não aponte o domínio para `NpgsqlDataSource` desta tabela.

## Instalação e builder

```bash
dotnet add package MB.ScopedDataSync.Storage.Postgres
dotnet add package MB.ScopedDataSync.Caching.Redis
```

```csharp
services.AddScopedDataSync<PositionSnapshot>(builder =>
{
    builder.WithScope("positions");
    builder.UseRedisCache(cache =>
    {
        cache.ConnectionString = configuration["Redis:ConnectionString"];
        cache.ServiceName = configuration["Redis:ServiceName"]!;
    });
    builder.UsePostgresStore(database =>
    {
        database.ConnectionString = configuration.GetConnectionString("EntitySync");
    });
});
```

## appsettings

```json
{
  "ConnectionStrings": {
    "EntitySync": "Host=postgres.internal;Port=5432;Database=positions;Username=positions;Password=change-me;SSL Mode=Prefer"
  }
}
```

`GetConnectionString("EntitySync")` devolve essa entrada no formato do Npgsql (`Host`, `Port`, `Database`, `Username`, `Password`, `SSL Mode`). Esse banco guarda `entity_scope_projections`. A outbox do produtor usa outra connection string, `EntityOutbox`, mesmo quando o servidor é o mesmo.

Só o PostgreSQL, sem cache, também é válido. `EnsureSchemaOnStartup` permanece `true` e cria `entity_scope_projections` com chave primária `(entity_id, scope)`. Passe `false` quando a migração do serviço já criou a tabela.

## Cerca de versão

```sql
ON CONFLICT (entity_id, scope)
DO UPDATE SET
    version = EXCLUDED.version,
    payload = EXCLUDED.payload,
    updated_at_utc = EXCLUDED.updated_at_utc,
    consecutive_failures = 0
WHERE entity_scope_projections.version < EXCLUDED.version
```

O statement devolve se a linha foi aplicada. Recusa não atualiza o cache. Um `INSERT ... ON CONFLICT DO UPDATE` sem o `WHERE` feito pela aplicação aceitaria versão antiga: não exponha a tabela a outro repositório.

Falhas entram assim, sem atribuir `version` nem `payload` no `SET`:

```sql
DO UPDATE SET
    consecutive_failures = entity_scope_projections.consecutive_failures + 1,
    last_failure_at_utc = EXCLUDED.last_failure_at_utc,
    last_failure_reason = EXCLUDED.last_failure_reason
```

A primeira falha, sem linha, insere `version = 0`. O próximo evento com versão positiva ainda passa na comparação.

Staleness, cooldown e o tópico compactado com chave `entityId` estão no `Consumer` e no `Producer`.
