using System.Text;
using LightningDB;

namespace KeyValue.Benchmarks.Stores;

public class LightningLmdbStore : IStore
{
    private readonly LightningEnvironment _env;

    public LightningLmdbStore()
    {
        _env = new LightningEnvironment("lightning_data");
        _env.Open();

        using var tx = _env.BeginTransaction();
        using var db = tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });

        var result = db.Truncate(tx);

        Console.WriteLine($"Truncate result: {result}");

        tx.Commit();
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

        var id = Guid.NewGuid();
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

    public unsafe ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key)
    {
        using var tx = _env.BeginTransaction();
        using var db = tx.OpenDatabase(configuration: new DatabaseConfiguration { Flags = DatabaseOpenFlags.Create });

        Span<byte> keyBytes = stackalloc byte[key.SpanSize];

        key.Write(keyBytes);


        var id = Guid.NewGuid();
        Span<byte> valueBytes = stackalloc byte[16];

        if(!id.TryWriteBytes(valueBytes))
        {
            throw new Exception("Error writing Guid bytes");
        }

        var result = tx.Put(db, keyBytes, valueBytes, PutOptions.NoOverwrite);

        if(result == MDBResultCode.KeyExist)
        {
            (result, _, var value)  =  tx.Get(db, keyBytes);

            if (result != MDBResultCode.Success)
            {
                throw new Exception($"Error getting value for key {key}. Result: {result}");
            }

            return ValueTask.FromResult(new Guid(value.AsSpan()));
        }

        tx.Commit();

        return ValueTask.FromResult(id);
    }


    public void Dispose()
    {
        _env.Dispose();
    }
}
