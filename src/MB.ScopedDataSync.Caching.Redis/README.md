# MB.ScopedDataSync.Caching.Redis

Cache Redis isolado por serviço e escopo. As chaves de metadados e de payload usam a hash tag `{entityId}` para cair no mesmo slot. Versionamento e falha consecutiva rodam em scripts Lua.

Instalar este pacote traz `Consumer` e, de forma transitiva, `Abstractions`.

## Quando usar

Várias instâncias do mesmo serviço precisam do mesmo snapshot quente, com cerca de versão compartilhada.

## Quando não usar

Não use como único registro durável se o payload precisa sobreviver à perda do Redis. Combine com `UsePostgresStore` ou use só o PostgreSQL. Não registre também `UseInMemoryCache`.

## Instalação e builder

```bash
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
        cache.Database = configuration.GetValue("Redis:Database", -1);
        var payloadTtl = configuration["Redis:PayloadTtl"];
        if (!string.IsNullOrWhiteSpace(payloadTtl))
        {
            cache.PayloadTtl = TimeSpan.Parse(payloadTtl);
        }
    });
});
```

## appsettings

```json
{
  "Redis": {
    "ConnectionString": "redis.internal:6379,password=change-me,ssl=True,abortConnect=false",
    "ServiceName": "positions-api",
    "Database": -1,
    "PayloadTtl": "00:30:00"
  }
}
```

`ConnectionString` é o formato do StackExchange.Redis (`host:port`, com `password`, `ssl` e `abortConnect` na mesma string). `Database` `-1` usa o banco padrão do multiplexer. De `0` a `15`, o valor vai para `GetDatabase`. `ServiceName` compõe a chave e permanece estável entre deploys do mesmo serviço. `PayloadTtl` em `hh:mm:ss` é o TTL do payload. Sem essa entrada, `:data` não recebe `PEXPIRE`.

As chaves são `mbsds:{entityId}:{service}:{scope}:meta` e `:data`. `PayloadTtl` aplica `PEXPIRE` só em `:data`. A hash `:meta` guarda `version` e não expira. `ServiceName` isola outro serviço no mesmo Redis.

## Atomicidade

`TryApply` faz `HGET` da versão e, se `current >= incoming`, devolve descarte sem escrever. Caso contrário atualiza meta e payload no mesmo script. `RecordFailure` usa `HINCRBY` em `consecutiveFailures` e não altera `version` quando a hash já existe. Os dois comandos recebem chaves da mesma hash tag, então o cluster aceita o script.

Não grave essas chaves por um cliente paralelo: o script deixa de ser a única cerca.

Leitura, staleness, cooldown e a proibição de upsert externo no PostgreSQL seguem o `Consumer` e o `Storage.Postgres`. A chave Kafka `entityId` e o payload `[]` seguem o `Producer`.
