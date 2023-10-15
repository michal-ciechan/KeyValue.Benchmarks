using System.Collections.Concurrent;
namespace KeyValue.Benchmarks.Stores;

public class ConcurrentDictionaryStore : IStore
{
    private readonly ConcurrentDictionary<TradeKey, Guid> _store;

    public ConcurrentDictionaryStore()
    {
        _store = new ConcurrentDictionary<TradeKey, Guid>();
    }

    public Guid GetOrCreateKey(TradeKey key)
    {
        throw new NotImplementedException();
    }

    public ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key)
    {
        var guid = _store.GetOrAdd(key, _ => Guid.NewGuid());
        return ValueTask.FromResult(guid);
    }


    public void Dispose()
    {
    }
}
