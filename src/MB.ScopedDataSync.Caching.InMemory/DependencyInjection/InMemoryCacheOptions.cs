namespace MB.ScopedDataSync.Caching.InMemory.DependencyInjection;

public sealed class InMemoryCacheOptions
{
    /// <summary>
    /// TTL do payload no <see cref="Microsoft.Extensions.Caching.Memory.IMemoryCache"/>.
    /// A watermark de <c>Version</c> não expira com o payload, para um evento atrasado
    /// não reaplicar depois do despejo.
    /// </summary>
    public TimeSpan? PayloadTtl { get; set; }
}
