using System.Collections.Concurrent;
namespace KeyValue.Benchmarks.Stores;

public class ConcurrentDictionaryStore : IStore
{
    private readonly ConcurrentDictionary<TradeKey, Guid> _store;

    public ConcurrentDictionaryStore()
    {
        _store = new ConcurrentDictionary<TradeKey, Guid>(TradeKey.TradeKeyComparer);
    }

    public Guid GetOrCreateKey(TradeKey key)
    {
        return _store.GetOrAdd(key, _ => Guid.NewGuid());
    }

    public ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key)
    {
        return ValueTask.FromResult(GetOrCreateKey(key));
    }


    public void Dispose()
    {
    }
}
