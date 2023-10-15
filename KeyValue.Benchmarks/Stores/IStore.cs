namespace KeyValue.Benchmarks.Stores;

public interface IStore : IDisposable
{
    Guid GetOrCreateKey(TradeKey key);
    ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key);
}
