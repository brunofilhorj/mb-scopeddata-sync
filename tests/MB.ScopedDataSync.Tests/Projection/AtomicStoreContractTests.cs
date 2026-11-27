using MB.ScopedDataSync.Caching.Redis.Scripting;
using MB.ScopedDataSync.Caching.Redis.Store;
using MB.ScopedDataSync.Storage.Postgres.Sql;

namespace MB.ScopedDataSync.Tests.Projection;

public class AtomicStoreContractTests
{
    [Fact]
    public void Redis_keys_share_the_entity_hash_tag_and_scripts_protect_version()
    {
        var keys = RedisKeyBuilder.For("positions-api", "positions", "inv-1");

        Assert.Contains("{inv-1}", keys.MetaKey, StringComparison.Ordinal);
        Assert.Contains("{inv-1}", keys.PayloadKey, StringComparison.Ordinal);
        Assert.EndsWith(":meta", keys.MetaKey, StringComparison.Ordinal);
        Assert.EndsWith(":data", keys.PayloadKey, StringComparison.Ordinal);
        Assert.Contains("tonumber(current) >= incoming", RedisAtomicScripts.TryApply, StringComparison.Ordinal);
        Assert.Contains("redis.call('PEXPIRE', KEYS[2]", RedisAtomicScripts.TryApply, StringComparison.Ordinal);
        Assert.DoesNotContain("redis.call('PEXPIRE', KEYS[1]", RedisAtomicScripts.TryApply, StringComparison.Ordinal);
        Assert.Contains("HINCRBY", RedisAtomicScripts.RecordFailure, StringComparison.Ordinal);
        Assert.DoesNotContain("KEYS[2]", RedisAtomicScripts.RecordFailure, StringComparison.Ordinal);
        Assert.DoesNotContain("PEXPIRE", RedisAtomicScripts.RecordFailure, StringComparison.Ordinal);
    }

    [Fact]
    public void Postgres_upsert_applies_only_newer_versions_and_failures_do_not_assign_version()
    {
        Assert.Contains(
            "WHERE entity_scope_projections.version < EXCLUDED.version",
            PostgresProjectionSql.TryApply,
            StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (entity_id, scope)", PostgresProjectionSql.TryApply, StringComparison.Ordinal);

        var failureUpdate = PostgresProjectionSql.RecordFailure.Split("DO UPDATE SET", StringSplitOptions.TrimEntries)[1];
        Assert.DoesNotContain("version", failureUpdate, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("payload", failureUpdate, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "consecutive_failures = entity_scope_projections.consecutive_failures + 1",
            failureUpdate,
            StringComparison.Ordinal);
        Assert.Contains("PRIMARY KEY (entity_id, scope)", PostgresProjectionSql.EnsureSchema, StringComparison.Ordinal);
    }
}
