using NUlid;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace KeyValue.Benchmarks.Stores;

public static class UlidExtensions
{
    public static Guid ToGuidFast(this in Ulid ulid)
    {
        Span<byte> destination = stackalloc byte[16];

        MemoryMarshal.TryWrite(destination, ref Unsafe.AsRef(in ulid));

        return new Guid(destination);
    }
}
