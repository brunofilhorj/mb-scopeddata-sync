namespace MB.ScopedDataSync.Abstractions.Projection;

/// <summary>
/// Marca um contrato que o domínio não deve receber. O registro em DI é keyed
/// e só o host consumidor resolve <see cref="IProjectionUpdater{TPayload}"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class EventConsumerOnlyAttribute : Attribute;
