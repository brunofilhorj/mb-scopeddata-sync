using System.Text.Json;

namespace MB.ScopedDataSync.Abstractions.Serialization;

/// <summary>
/// Opções de JSON compartilhadas entre produtor, cache e PostgreSQL para o mesmo payload.
/// </summary>
public static class ScopedDataSyncJson
{
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web);
}
