using MB.ScopedDataSync.Caching.InMemory.DependencyInjection;
using MB.ScopedDataSync.Consumer.DependencyInjection;
using MB.ScopedDataSync.Observability.DependencyInjection;
using MB.ScopedDataSync.Observability.Metrics;
using MB.ScopedDataSync.ObservabilityLocal;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);
builder.Logging.SetMinimumLevel(LogLevel.Information);

builder.Services.AddScopedDataSyncObservability();
builder.Services.AddScopedDataSync<PositionSnapshot>(sync =>
{
    sync.WithScope("positions");
    sync.WithStaleness(options =>
    {
        options.StaleAfter = TimeSpan.FromMinutes(2);
        options.AlertCooldown = TimeSpan.FromSeconds(20);
    });
    sync.UseInMemoryCache();
});
builder.Services.AddHostedService<DemoWorkload>();
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter(ScopedDataSyncTelemetry.Name)
        .AddPrometheusExporter());

var app = builder.Build();
app.MapPrometheusScrapingEndpoint();
app.MapGet("/", () => Results.Text("metrics at /metrics"));
app.Run("http://0.0.0.0:5088");
