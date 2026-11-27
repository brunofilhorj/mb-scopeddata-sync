namespace MB.ScopedDataSync.Caching.Redis.Store;

/// <summary>
/// As duas chaves carregam a hash tag <c>{entityId}</c> para cair no mesmo slot
/// e permitir script Lua atômico em cluster.
/// </summary>
public static class RedisKeyBuilder
{
    public static (string MetaKey, string PayloadKey) For(string serviceName, string scope, string entityId)
    {
        SyncNameRules.ThrowIfInvalidServiceName(serviceName);
        SyncNameRules.ThrowIfInvalidScope(scope);
        SyncNameRules.ThrowIfInvalidEntityId(entityId);
        var prefix = $"mbsds:{{{entityId}}}:{serviceName}:{scope}";
        return ($"{prefix}:meta", $"{prefix}:data");
    }
}
