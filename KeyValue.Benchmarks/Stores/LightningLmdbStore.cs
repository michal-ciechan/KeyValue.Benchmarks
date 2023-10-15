using System.Text;
using LightningDB;
using NUlid;

namespace KeyValue.Benchmarks.Stores;

public class LightningLmdbStore : IStore
{
    private readonly LightningEnvironment _env;

    public LightningLmdbStore()
    {
        _env = new LightningEnvironment("lightning_data");
        _env.Open();
    }

    public Guid GetOrCreateKey(TradeKey key)
    {
        using var tx = _env.BeginTransaction();
        using var db = tx.OpenDatabase(
            configuration: new DatabaseConfiguration
            {
                Flags = DatabaseOpenFlags.Create
            }
        );

        Span<byte> keyBytes = stackalloc byte[key.SpanSize];

        key.Write(keyBytes);

        var id = Ulid.NewUlid().ToGuidFast();

        Span<byte> valueBytes = stackalloc byte[16];

        if (!id.TryWriteBytes(valueBytes))
        {
            throw new Exception("Error writing Guid bytes");
        }

        var result = tx.Put(db, keyBytes, valueBytes, PutOptions.NoOverwrite);

        if (result == MDBResultCode.KeyExist)
        {
            (result, _, var value) = tx.Get(db, keyBytes);

            if (result != MDBResultCode.Success)
            {
                throw new Exception($"Error getting value for key {key}. Result: {result}");
            }

            return new Guid(value.AsSpan());
        }

        tx.Commit();

        return id;
    }

    public ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key)
    {
        return ValueTask.FromResult(GetOrCreateKey(key));
    }

    public void Cleanup()
    {
        using var tx = _env.BeginTransaction();
        using var db = tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });

        var result = db.Truncate(tx);

        Console.WriteLine($"Truncate result: {result}");

        tx.Commit();
    }

    public void Recover()
    {
    }

    public void Dispose()
    {
        _env.Dispose();
    }
}
