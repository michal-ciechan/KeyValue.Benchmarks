using BenchmarkDotNet.Attributes;
using KeyValue.Benchmarks;
using KeyValue.Benchmarks.Stores;
using Testcontainers.PostgreSql;

public class SingleOperationBenchmarks
{
    private IStore _store;
    private List<TradeKey> _keys;

    [ParamsAllValues]
    public StoresEnum StoreEnum { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _keys = Enumerable.Range(1,1000).Select(x => new TradeKey
        {
            TradeDate = new DateOnly(2021, 1, 1).AddDays(Random.Shared.Next(0, 365)),
            ExchangeLinkId = Guid.NewGuid().ToString(),
            ExchangeTradeId = Guid.NewGuid().ToString(),
        }).ToList();


        _store = StoreEnum switch
        {
            StoresEnum.RedisFsync1Sec => new RedisTradeKeyStore(StoreEnum),
            StoresEnum.RedisFsyncAlways => new RedisTradeKeyStore(StoreEnum),
            StoresEnum.Redis => new RedisTradeKeyStore(StoreEnum),
            StoresEnum.Postgres => new PostgresStore(),
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _store?.Dispose();
    }

    // Benchmarks

    [Benchmark]
    public Guid Redis_SingleGetOrAdd_Fsync()
    {
        return RunSingleLoop(_store.GetOrCreateKey);
    }

    [Benchmark]
    public Guid Redis_SingleGetOrAdd_1Sec()
    {
        return RunSingleLoop(_store.GetOrCreateKey);
    }

    // Methods
    private Guid RunSingleLoop(Func<TradeKey, Guid> generator)
    {
        var key = _keys[Random.Shared.Next(0, _keys.Count)];

        var id = generator(key);

        return id;
    }
}
