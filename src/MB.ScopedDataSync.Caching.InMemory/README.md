# MB.ScopedDataSync.Caching.InMemory

Cache de processo com `IMemoryCache`. A watermark de `Version` fica fora do TTL: depois que o payload expira, um evento atrasado continua sendo recusado.

Instalar este pacote traz `MB.ScopedDataSync.Consumer` e, de forma transitiva, `MB.ScopedDataSync.Abstractions`.

## Quando usar

Um processo por vez, teste local ou projeção cuja perda no restart é aceitável porque o store de banco, quando configurado, reidrata.

## Quando não usar

Várias instâncias do serviço. Nesse caso use `MB.ScopedDataSync.Caching.Redis`. Não combine este cache com Redis no mesmo `AddScopedDataSync<T>`.

## Instalação e builder

```bash
dotnet add package MB.ScopedDataSync.Caching.InMemory
```

```csharp
services.AddScopedDataSync<PositionSnapshot>(builder =>
{
    builder.WithScope("positions");
    builder.UseInMemoryCache(cache => cache.PayloadTtl = TimeSpan.FromMinutes(15));
    builder.UsePostgresStore(database =>
        database.ConnectionString = configuration.GetConnectionString("EntitySync"));
});
```

`PayloadTtl` nulo mantém o payload até o despejo do processo. A chave de payload é `mbsds:{scope}:{entityId}`.

## Leitura, versão e falha

O domínio usa `IScopeReader<T>`. O consumidor aplica pelo updater keyed. Sob o lock da entidade, `TryApply` grava só quando `incoming.Version > current.Version`. `RecordFailure` incrementa o contador e preserva versão e payload. Staleness e cooldown são do `Consumer`.

Não atualize o `IMemoryCache` por fora com a mesma chave: a watermark do store deixaria de ser a cerca. Passe por `ApplyEventAsync`.

O produtor e a configuração `cleanup.policy=compact` do tópico ficam em `MB.ScopedDataSync.Producer`.
