# MB.ScopedDataSync.Observability

Métricas (`System.Diagnostics.Metrics`) e spans (`ActivitySource`) do fluxo de projeção. O nome dos dois instrumentos é `MB.ScopedDataSync`.

O pacote referencia `Abstractions` de forma transitiva para substituir `IScopedDataSyncInstrumentation`. Não referencia OpenTelemetry, Prometheus nem Grafana. O host escolhe o exporter. O dashboard e as regras Prometheus viajam dentro deste pacote.

## Quando usar

Quando o serviço precisa contar eventos aplicados, descartados, falhas de sync, alertas de staleness e a idade da projeção, e expor isso ao Prometheus ou a um backend OTLP.

## Quando não usar

Não é obrigatório. Sem este registro, `AddScopedDataSync<T>` usa a implementação no-op. Não coloque `entityId` em tag: os instrumentos usam `scope` e, na idade, o status de frescor.

## Instalação

```bash
dotnet add package MB.ScopedDataSync.Observability
dotnet add package MB.ScopedDataSync.Consumer
dotnet add package MB.ScopedDataSync.Caching.InMemory
```

```csharp
services.AddScopedDataSyncObservability();
services.AddScopedDataSync<PositionSnapshot>(builder =>
{
    builder.WithScope("positions");
    builder.WithStaleness(options =>
    {
        options.StaleAfter = TimeSpan.FromMinutes(2);
        options.AlertCooldown = TimeSpan.FromMinutes(10);
    });
    builder.UseInMemoryCache();
});
```

A extensão remove o no-op e registra `MetricsScopedDataSyncInstrumentation`. A ordem em relação a `AddScopedDataSync<T>` não muda o resultado.

## Exemplo

Escopo `positions`, `StaleAfter` de 2 minutos e `AlertCooldown` de 10 minutos. A projeção corrente está na versão 7, com payload gravado agora. Os contadores sobem em 1 por chamada. A tag de todos eles é `scope=positions`.

1. O consumidor aplica um envelope de versão 8. O contador `mb.scoped_data_sync.events.applied` incrementa. O span `entity.projection.apply` termina com `entity.outcome=applied`. A versão corrente passa a ser 8.
2. O mesmo envelope da versão 8 chega de novo. A cerca `incoming.Version > current.Version` recusa. `mb.scoped_data_sync.events.discarded` incrementa e o span termina com `entity.outcome=discarded`. A versão e o payload ficam em 8.
3. O domínio lê 30 segundos depois. A idade cabe em `StaleAfter`, então o status é `Fresh`. `mb.scoped_data_sync.projection.age` registra cerca de 30000, com tags `scope=positions` e `status=Fresh`. Não há alerta.
4. O domínio lê de novo 3 minutos depois da gravação. A idade passou de `StaleAfter`, o status é `Stale` e o histograma registra essa idade com `status=Stale`. O cooldown está livre, o sink publica o alerta e `mb.scoped_data_sync.staleness.alerts` incrementa.
5. Outra leitura um minuto depois continua `Stale`. O histograma registra de novo. O cooldown da chave `positions:{entityId}` ainda segura o alerta, então `staleness.alerts` não incrementa e `FreshnessInfo.AlertSuppressedByCooldown` volta `true`.
6. `RecordSyncFailureAsync` grava a falha. `mb.scoped_data_sync.sync.failures` incrementa e o span `entity.projection.sync_failure` termina com `entity.outcome=recorded`. A versão e o payload aceito não mudam. Se a próxima leitura ainda estiver dentro de `StaleAfter` e `ConsecutiveFailures` for maior que zero, o status é `SyncImpaired` e o histograma usa essa tag. `SyncImpaired` não publica alerta de staleness.
7. Uma leitura sem payload (status `Missing`) não registra o histograma: a idade é nula. O span `entity.projection.read` ainda termina com `entity.outcome=Missing`.

Uma exceção no apply, na leitura ou na gravação da falha marca o span com `ActivityStatusCode.Error`, `entity.outcome=error` e a exceção. O contador daquele passo não incrementa, porque o incremento só acontece depois da gravação bem-sucedida.

## Sinais

| Instrumento | Nome no `Meter` e no OTLP | Nome no Prometheus |
| --- | --- | --- |
| Contador de apply | `mb.scoped_data_sync.events.applied` | `mb_scoped_data_sync_events_applied_total` |
| Contador de descarte | `mb.scoped_data_sync.events.discarded` | `mb_scoped_data_sync_events_discarded_total` |
| Contador de falha de sync | `mb.scoped_data_sync.sync.failures` | `mb_scoped_data_sync_sync_failures_total` |
| Contador de alerta de staleness | `mb.scoped_data_sync.staleness.alerts` | `mb_scoped_data_sync_staleness_alerts_total` |
| Histograma da idade, unidade `ms` | `mb.scoped_data_sync.projection.age` | `mb_scoped_data_sync_projection_age_milliseconds` |

Os limites do histograma, em milissegundos, são 100, 500, 1s, 5s, 15s, 30s, 1min, 2min, 5min, 15min, 1h, 6h e 1d. O p95 de uma projeção com vários minutos de idade continua legível. O conjunto padrão do runtime para em 10 segundos.

A coluna Prometheus vale para a tradução padrão do exporter (`UnderscoreEscapingWithSuffixes`): ponto vira `_`, contador ganha `_total` e a unidade `ms` vira o sufixo `_milliseconds`. O histograma aparece como `_bucket`, `_sum` e `_count`. Um backend que guarda o nome OTLP usa a coluna do meio, com ponto e sem esses sufixos.

Tags de contador: `scope`. Tags do histograma: `scope` e `status` (`Fresh`, `Stale` ou `SyncImpaired`).

Spans: `entity.projection.read`, `entity.projection.apply` e `entity.projection.sync_failure`, com tag `entity.scope` e resultado em `entity.outcome`.

O descarte continua sendo a cerca `incoming.Version > current.Version`. O alerta de staleness continua passando pelo cooldown do `Consumer`. Postgres opcional e o produtor Kafka não mudam com este pacote. Tópico `cleanup.policy=compact`, chave `entityId` e payload `[]` estão no `Producer`.

## Prometheus e OTLP

O host adiciona os pacotes de exporter. Este pacote não os traz.

Aplicação ASP.NET Core, scrape em `/metrics` e trace por OTLP:

```bash
dotnet add package OpenTelemetry.Extensions.Hosting
dotnet add package OpenTelemetry.Exporter.Prometheus.AspNetCore
dotnet add package OpenTelemetry.Exporter.OpenTelemetryProtocol
```

```csharp
using MB.ScopedDataSync.Observability.Metrics;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(ScopedDataSyncTelemetry.Name)
        .AddPrometheusExporter())
    .WithTracing(tracing => tracing
        .AddSource(ScopedDataSyncTelemetry.Name)
        .AddOtlpExporter());

app.MapPrometheusScrapingEndpoint();
```

`AddMeter` e `AddSource` recebem `MB.ScopedDataSync`. Sem esse nome, o scrape e o coletor não veem estes sinais. `OTEL_EXPORTER_OTLP_ENDPOINT` aponta o trace (e, se o host também chamar `AddOtlpExporter` em métricas, as métricas) para o coletor. Grafana, Datadog, New Relic e Elastic consomem esse OTLP. O Prometheus continua no scrape de `/metrics`, ou no remote write do coletor.

Worker sem ASP.NET Core troca o exporter HTTP. O pacote é `OpenTelemetry.Exporter.Prometheus.HttpListener` e o padrão escuta `http://localhost:9464/`.

```csharp
using OpenTelemetry.Metrics;

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(ScopedDataSyncTelemetry.Name)
        .AddPrometheusHttpListener());
```

Consultas no Prometheus, depois do exemplo acima. A taxa de apply por escopo:

```promql
sum by (scope) (rate(mb_scoped_data_sync_events_applied_total[5m]))
```

A idade p95, em milissegundos, por escopo:

```promql
histogram_quantile(
  0.95,
  sum by (le, scope) (
    rate(mb_scoped_data_sync_projection_age_milliseconds_bucket[5m])
  )
)
```

No armazenamento OTLP com o nome original, a mesma idade p95 usa `mb.scoped_data_sync.projection.age`.

## Dashboard e alertas

Os dois arquivos estão no repositório e entram no nupkg:

- `src/MB.ScopedDataSync.Observability/dashboards/grafana-scoped-data-sync.json`
- `src/MB.ScopedDataSync.Observability/alerts/prometheus-scoped-data-sync.yml`

No pacote instalado, os caminhos são `dashboards/grafana-scoped-data-sync.json` e `alerts/prometheus-scoped-data-sync.yml`. Na versão 1.0.0, o cache local do NuGet fica em `~/.nuget/packages/mb.scopeddata-sync.observability/1.0.0/`.

No Grafana: Dashboards, Import, e selecione o JSON. A importação pede o datasource Prometheus. A primeira faixa são totais da janela selecionada (`increase` sobre `$__range`): aplicados, descartados, falhas, alertas de staleness e leituras com payload. Ao lado, a proporção de descarte e a proporção de leitura `Stale`. Abaixo ficam as taxas por segundo, o mix de frescor (`Fresh`, `Stale`, `SyncImpaired`) e a idade p50 e p95 por status. Leitura `Missing` não entra nos totais de leitura nem no mix. O contador cru zera no restart do processo; o `increase` da janela não.

## Ver no Grafana

`samples/MB.ScopedDataSync.ObservabilityLocal` é um host ASP.NET Core na porta 5088. A cada 2 segundos ele aplica uma versão nova de `inv-fresh`, reentrega a versão 1 (descarte), grava falha em `inv-impaired` e lê `inv-stale`, cuja projeção está 10 minutos atrás. `StaleAfter` continua 2 minutos. O cooldown do exemplo local é 20 segundos, para o painel de alerta se mover durante a sessão. O Prometheus e o Grafana sobem com Docker e importam o JSON deste pacote.

```bash
dotnet run --project samples/MB.ScopedDataSync.ObservabilityLocal
docker compose -f samples/MB.ScopedDataSync.ObservabilityLocal/docker-compose.yml up -d
```

O scrape é `http://localhost:5088/metrics`. O Grafana abre em `http://localhost:3000` (anônimo como Admin) e o Prometheus em `http://localhost:9090`. Na importação do dashboard, o datasource provisionado tem uid `prometheus`.

```bash
python3 - <<'PY'
import json, urllib.request
dashboard = json.load(open("src/MB.ScopedDataSync.Observability/dashboards/grafana-scoped-data-sync.json"))
body = json.dumps({
    "dashboard": dashboard,
    "overwrite": True,
    "inputs": [{
        "name": "DS_PROMETHEUS",
        "type": "datasource",
        "pluginId": "prometheus",
        "value": "prometheus",
    }],
}).encode()
request = urllib.request.Request(
    "http://localhost:3000/api/dashboards/import",
    data=body,
    headers={"Content-Type": "application/json"},
)
print(urllib.request.urlopen(request).read().decode())
PY
```

O painel fica em `http://localhost:3000/d/mb-scoped-data-sync/mb-scoped-data-sync`. As regras do pacote já entram no Prometheus desse compose; os alertas de 10 e 15 minutos ficam pendentes até cumprirem o `for`.

Para encerrar o Prometheus e o Grafana:

```bash
docker compose -f samples/MB.ScopedDataSync.ObservabilityLocal/docker-compose.yml down
```

O host na porta 5088 é o processo do `dotnet run`. Ctrl+C encerra esse processo.

No Prometheus, o arquivo de regras entra em `rule_files`. As regras só leem séries. Apply, descarte, versão e cooldown não mudam. Há seis alertas:

- `ScopedDataSyncFailures`: taxa de `sync.failures` maior que zero por 10 minutos.
- `ScopedDataSyncStaleAlerts`: taxa de `staleness.alerts` maior que zero por 15 minutos. O cooldown já filtrou o alerta por entidade; o que resta é projeção que segue além de `StaleAfter`.
- `ScopedDataSyncProjectionAgeHigh`: p95 da idade das leituras `Stale` acima de 5 minutos por 15 minutos. O limiar de exemplo fica acima de um `StaleAfter` de 2 minutos. Ajuste o número ao `StaleAfter` do serviço.
- `ScopedDataSyncStaleReadShare`: mais da metade das leituras com payload está `Stale` por 15 minutos. O limiar `0.5` é exemplo.
- `ScopedDataSyncDiscardsDominate`: por 30 minutos o descarte passa de 95% e o apply fica abaixo de 0.01/s. Replay curto do tópico compactado não cumpre essa janela.
- `ScopedDataSyncSignalMissing`: o contador de aplicados some do scrape por 15 minutos. Em Prometheus compartilhado, restrinja com `{job="nome-do-servico"}`. Este alerta não tem label `scope`.
