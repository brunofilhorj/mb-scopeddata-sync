using System.Collections;
using System.Text.Json;

namespace MB.ScopedDataSync.Producer.Publishing;

/// <summary>
/// Serializa o payload do evento. Coleção nula ou vazia vira <c>[]</c>.
/// Valor JSON <c>null</c> seria um tombstone em tópico <c>cleanup.policy=compact</c> e apagaria a chave da entidade.
/// </summary>
public static class PayloadNormalizer
{
    public static string ToJson<TPayload>(TPayload? payload)
    {
        if (payload is null)
        {
            if (IsJsonArrayType(typeof(TPayload)))
            {
                return "[]";
            }

            throw new ArgumentNullException(
                nameof(payload),
                "Null payloads are not published. A compacted topic would treat them as tombstones.");
        }

        if (payload is IEnumerable and not string && !payload.GetType().IsArray && CountIfCollection(payload) == 0)
        {
            return "[]";
        }

        var json = JsonSerializer.Serialize(payload, ScopedDataSyncJson.Options);
        if (json == "null")
        {
            throw new InvalidOperationException("Refusing to publish a JSON null payload because it is a Kafka tombstone.");
        }

        return json;
    }

    private static bool IsJsonArrayType(Type type)
    {
        if (type == typeof(string))
        {
            return false;
        }

        if (type.IsArray)
        {
            return true;
        }

        if (!typeof(IEnumerable).IsAssignableFrom(type))
        {
            return false;
        }

        return type.IsGenericType;
    }

    private static int CountIfCollection(object payload)
        => payload is ICollection collection ? collection.Count : -1;
}
