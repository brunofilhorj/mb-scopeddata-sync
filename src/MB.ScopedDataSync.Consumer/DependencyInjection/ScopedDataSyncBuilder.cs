using MB.ScopedDataSync.Consumer.Projection;
using MB.ScopedDataSync.Consumer.Staleness;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace MB.ScopedDataSync.Consumer.DependencyInjection;

public sealed class ScopedDataSyncBuilder<TPayload>
{
    private string? _scope;
    private Action<StalenessOptions>? _configureStaleness;
    private TimeProvider? _timeProvider;
    private CacheProviderRegistration<TPayload>? _cache;
    private DatabaseStoreRegistration<TPayload>? _database;
    private readonly List<Action<IServiceCollection, ScopedDataSyncRegistrationContext>> _eventSources = [];

    public ScopedDataSyncBuilder<TPayload> WithScope(string scope)
    {
        SyncNameRules.ThrowIfInvalidScope(scope);
        _scope = scope;
        return this;
    }

    public ScopedDataSyncBuilder<TPayload> WithStaleness(Action<StalenessOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _configureStaleness = configure;
        return this;
    }

    public ScopedDataSyncBuilder<TPayload> UseTimeProvider(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        return this;
    }

    /// <summary>
    /// Ponto de extensão usado por <c>UseInMemoryCache</c> e <c>UseRedisCache</c>.
    /// A segunda chamada lança porque só um cache participa da leitura.
    /// </summary>
    public ScopedDataSyncBuilder<TPayload> UseCacheProvider(CacheProviderRegistration<TPayload> registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        if (_cache is not null)
        {
            throw new InvalidOperationException(
                "At most one cache provider is allowed. Configure UseInMemoryCache or UseRedisCache, not both.");
        }

        _cache = registration;
        return this;
    }

    /// <summary>
    /// Ponto de extensão de um store de banco, relacional ou NoSQL.
    /// A segunda chamada lança porque só um banco participa da composição.
    /// </summary>
    public ScopedDataSyncBuilder<TPayload> UseDatabaseStore(DatabaseStoreRegistration<TPayload> registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        if (_database is not null)
        {
            throw new InvalidOperationException("Database store is already configured.");
        }

        _database = registration;
        return this;
    }

    /// <summary>
    /// Ponto de extensão de uma fonte de eventos.
    /// O adaptador registra o que alimenta <see cref="IProjectionUpdater{TPayload}"/>.
    /// Sem adaptador, o serviço aplica o envelope com o próprio job.
    /// </summary>
    public ScopedDataSyncBuilder<TPayload> UseEventSource(
        Action<IServiceCollection, ScopedDataSyncRegistrationContext> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        _eventSources.Add(configure);
        return this;
    }

    internal void Register(IServiceCollection services)
    {
        if (_cache is null && _database is null)
        {
            throw new InvalidOperationException(
                "Configure UseInMemoryCache or UseRedisCache, or a database store such as UsePostgresStore. At most one cache and one database store are allowed, and at least one of them is required.");
        }

        var scope = _scope ?? typeof(TPayload).Name;
        SyncNameRules.ThrowIfInvalidScope(scope);
        var staleness = new StalenessOptions();
        _configureStaleness?.Invoke(staleness);
        if (staleness.StaleAfter <= TimeSpan.Zero)
        {
            throw new InvalidOperationException("StaleAfter must be greater than zero.");
        }

        if (staleness.AlertCooldown < TimeSpan.Zero)
        {
            throw new InvalidOperationException("AlertCooldown cannot be negative.");
        }

        var context = new ScopedDataSyncRegistrationContext { Scope = scope };
        _cache?.ConfigureServices(services, context);
        _database?.ConfigureServices(services, context);
        foreach (var configureSource in _eventSources)
        {
            configureSource(services, context);
        }

        services.TryAddSingleton<IAlertCooldownGate, InMemoryAlertCooldownGate>();
        services.TryAddSingleton<IStaleAlertSink>(NullStaleAlertSink.Instance);
        services.TryAddSingleton<IScopedDataSyncErrorHandler>(NullScopedDataSyncErrorHandler.Instance);
        services.TryAddSingleton<IScopedDataSyncInstrumentation>(NoOpScopedDataSyncInstrumentation.Instance);
        services.TryAddSingleton(TimeProvider.System);

        var cacheRegistration = _cache;
        var databaseRegistration = _database;
        var timeProvider = _timeProvider;

        services.AddSingleton(serviceProvider =>
        {
            var cache = cacheRegistration?.CreateStore(serviceProvider, context);
            var database = databaseRegistration?.CreateStore(serviceProvider, context);
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
            var logger = loggerFactory?.CreateLogger($"MB.ScopedDataSync.Consumer.ProjectionCoordinator[{typeof(TPayload).Name}]")
                ?? Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

            return new ProjectionCoordinator<TPayload>(
                scope,
                staleness,
                cache,
                database,
                serviceProvider.GetRequiredService<IAlertCooldownGate>(),
                serviceProvider.GetRequiredService<IStaleAlertSink>(),
                serviceProvider.GetRequiredService<IScopedDataSyncErrorHandler>(),
                serviceProvider.GetRequiredService<IScopedDataSyncInstrumentation>(),
                timeProvider ?? serviceProvider.GetRequiredService<TimeProvider>(),
                logger);
        });

        services.AddSingleton<IScopeReader<TPayload>>(serviceProvider =>
            new ScopeReadGateway<TPayload>(serviceProvider.GetRequiredService<ProjectionCoordinator<TPayload>>()));

        services.AddKeyedSingleton<IProjectionUpdater<TPayload>>(
            ScopedDataSyncServiceKeys.EventConsumer,
            (serviceProvider, _) => new ProjectionWriteGateway<TPayload>(
                serviceProvider.GetRequiredService<ProjectionCoordinator<TPayload>>()));
    }
}
