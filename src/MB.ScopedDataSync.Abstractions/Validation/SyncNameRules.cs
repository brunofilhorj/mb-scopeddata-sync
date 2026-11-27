namespace MB.ScopedDataSync.Abstractions.Validation;

/// <summary>
/// Regras de identificadores usados em chave de partição, hash tag Redis e chave primária.
/// </summary>
public static class SyncNameRules
{
    public const int MaxLength = 128;

    public static void ThrowIfInvalidEntityId(string? entityId, string paramName = "entityId")
        => ThrowIfInvalid(entityId, paramName, "Entity id");

    public static void ThrowIfInvalidScope(string? scope, string paramName = "scope")
        => ThrowIfInvalid(scope, paramName, "Scope");

    public static void ThrowIfInvalidServiceName(string? serviceName, string paramName = "serviceName")
        => ThrowIfInvalid(serviceName, paramName, "Service name");

    private static void ThrowIfInvalid(string? value, string paramName, string label)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > MaxLength || value.AsSpan().ContainsAny("{}: \t\r\n"))
        {
            throw new ArgumentException(
                $"{label} must be 1..{MaxLength} characters and cannot contain braces, colon or whitespace.",
                paramName);
        }
    }
}
