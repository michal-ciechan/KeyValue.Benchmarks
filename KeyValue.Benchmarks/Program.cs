﻿// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using Humanizer;
using KeyValue.Benchmarks.Stores;
using Perfolizer.Horology;
using System.Collections.Immutable;

Console.WriteLine("Hello, World!");

var sw = Stopwatch.StartNew();

// BenchmarkRunner.Run<Benchmarks>(new Config());
BenchmarkRunner.Run<Benchmarks>(new CustomDebugConfig());

Console.WriteLine($"Done in {sw.Elapsed.Humanize()}");





class Config : ManualConfig
{
    public Config()
    {
        Add(DefaultConfig.Instance.GetExporters().ToArray());
        Add(DefaultConfig.Instance.GetLoggers().ToArray());
        Add(DefaultConfig.Instance.GetColumnProviders().ToArray());

        SummaryStyle = BenchmarkDotNet.Reports.SummaryStyle.Default.WithTimeUnit(TimeUnit.Millisecond);

        var filter = new SimpleFilter(
            x =>
            {
                var regex = @"\[Count=(?<Count>\d+), Store=(?<Store>\w+)\, Key=(?<Key>\w+)\]";

                var match = Regex.Match(x.Parameters.ValueInfo, regex);

                // throw if not success
                if (!match.Success)
                {
                    throw new Exception(
                        $"Failed to parse {x.Parameters.ValueInfo}. " +
                        $"Example expected value is [Count=10000, Store=RedisFsync1Sec, Key=Random]"
                    );
                }

                var parallelIterationCount = int.Parse(match.Groups["Count"].Value);
                var store = Enum.Parse<StoresEnum>(match.Groups["Store"].Value);
                var key = Enum.Parse<KeyRandomness>(match.Groups["Key"].Value);

                // return store == Stores.Postgres && parallelIterationCount > 1;
                // return store == StoresEnum.Lightning && parallelIterationCount == 1;

                if (parallelIterationCount > 10_000)
                {
                    switch (store)
                    {
                        case StoresEnum.RedisFsyncAlways: // Timesout @ 100k
                        case StoresEnum.RedisFsync1Sec: // Timesout @ 100k
                        case StoresEnum.Redis: // Timesout @ 100k
                            return false;
                    }
                }

                return true;
            }
        );

        AddFilter(filter);
    }

    // public class Orderer : DefaultOrderer
    // {
    //     public override IEnumerable<BenchmarkCase> GetSummaryOrder(ImmutableArray<BenchmarkCase> benchmarksCases, Summary summary)
    //     {
    //         return benchmarksCases;
    //     }
    //
    //     public override string? GetHighlightGroupKey(BenchmarkCase benchmarkCase)
    //     {
    //         throw new NotImplementedException();
    //     }
    //
    //     public string? GetLogicalGroupKey(ImmutableArray<BenchmarkCase> allBenchmarksCases, BenchmarkCase benchmarkCase)
    //     {
    //         throw new NotImplementedException();
    //     }
    //
    //     public IEnumerable<IGrouping<string, BenchmarkCase>> GetLogicalGroupOrder(IEnumerable<IGrouping<string, BenchmarkCase>> logicalGroups, IEnumerable<BenchmarkLogicalGroupRule>? order = null)
    //     {
    //         throw new NotImplementedException();
    //     }
    //
    //     public bool SeparateLogicalGroups { get; set; }
    // }
}

class CustomDebugConfig : Config, IConfig
{
    // DebuggableConfig
    IEnumerable<Job> IConfig.GetJobs() => (IEnumerable<Job>)new Job[1]
    {
        JobMode<Job>.Default.WithToolchain((IToolchain)new InProcessEmitToolchain(TimeSpan.FromHours(1.0), true)),
    };
}


// [SimpleJob(RunStrategy.Monitoring, iterationCount: 10)]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByMethod)]
[MinColumn, MaxColumn, MeanColumn, MedianColumn]
public class Benchmarks
{
    private List<TradeKey> _keys;
    private IStore _store;
    private Guid[] _res;

    // [Params(1, 10_000)]
    [Params(10_000)]
    // [Params(1)]
    public int Count { get; set; }

    // [ParamsAllValues]
    // [Params(StoresEnum.RedisFsyncAlways)]
    // [Params(StoresEnum.Lightning, StoresEnum.FasterKVSpanByte, StoresEnum.ConcurrentDictionary)]
    [Params(StoresEnum.FasterKVSpanByte)]
    public StoresEnum Store { get; set; }

    // [ParamsAllValues]
    [Params(KeyRandomness.Random)]
    public KeyRandomness Key { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _keys = Enumerable.Range(1, 1000)
            .Select(_ => CreateTradeKey())
            .ToList();

        _res = new Guid[Count];

        var storeType = Store;

        _store = CreateStore(storeType);

        // Verify Correctness
        var id1 = _store.GetOrCreateKeyAsync(_keys[0]).Result;
        var id2 = _store.GetOrCreateKeyAsync(_keys[0]).Result;

        if(id1 != id2)
        {
            throw new Exception("Id1 != Id2");
        }
    }

    public static IStore CreateStore(StoresEnum storeType)
    {
        return storeType switch
        {
            StoresEnum.RedisFsync1Sec       => new RedisTradeKeyStore(storeType),
            StoresEnum.RedisFsyncAlways     => new RedisTradeKeyStore(storeType),
            StoresEnum.Redis                => new RedisTradeKeyStore(storeType),
            StoresEnum.Postgres             => new PostgresStore(),
            StoresEnum.Lightning            => new LightningLmdbStore(),
            StoresEnum.FasterKV             => new FasterKvStore(storeType),
            StoresEnum.FasterKVSerialiser   => new FasterKvStore(storeType),
            StoresEnum.FasterKVNoCommit     => new FasterKvStore(storeType),
            StoresEnum.FasterKVSpanByte     => new FasterKvStoreSpanByte(storeType),
            StoresEnum.ConcurrentDictionary => new ConcurrentDictionaryStore(),
            _                               => throw new ArgumentOutOfRangeException()
        };
    }

    private static TradeKey CreateTradeKey()
    {
        return new TradeKey
        {
            TradeDate = new DateOnly(2021, 1, 1).AddDays(Random.Shared.Next(0, 365)),
            ExchangeLinkId = Guid.NewGuid().ToString(),
            ExchangeTradeId = Guid.NewGuid().ToString(),
        };
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _store.Dispose();
    }

    [Benchmark]
    public bool EnumerableSync()
    {
        return RunEnumerableLoop(key => _store.GetOrCreateKey(key));
    }

    [Benchmark]
    public bool EnumerableAsync()
    {
        return RunEnumerableAsyncLoop(key => _store.GetOrCreateKeyAsync(key)).GetAwaiter().GetResult();
    }

    [Benchmark]
    public bool ParallelSync()
    {
        return RunParallelLoop(key => _store.GetOrCreateKey(key));
    }

    [Benchmark]
    public bool ParallelAsync()
    {
        return RunParallelAsyncLoop(key => _store.GetOrCreateKeyAsync(key)).GetAwaiter().GetResult();
    }

    private bool RunEnumerableLoop(Func<TradeKey, Guid> generator)
    {
        for (var i = 0; i < Count; i++)
        {
            var key = GetKey(i);

            var id = generator(key);

            _res[i] = id;
        }

        return VerifyCorrectness();
    }

    private async Task<bool> RunEnumerableAsyncLoop(Func<TradeKey, ValueTask<Guid>> generator)
    {
        var tasks = Enumerable.Range(0, Count)
            .Select(
                async i =>
                {
                    var key = GetKey(i);

                    var id = await generator(key);

                    return id;
                }
            )
            .ToList();

        _res = await Task.WhenAll(tasks);

        return VerifyCorrectness();
    }

    private bool RunParallelLoop(Func<TradeKey, Guid> generator)
    {
        Parallel.For(0, Count,
            i =>
            {
                var key = GetKey(i);

                var id = generator(key);

                _res[i] = id;
            }
        );

        return VerifyCorrectness();
    }


    private async Task<bool> RunParallelAsyncLoop(Func<TradeKey, ValueTask<Guid>> generator)
    {
        var keys = Enumerable.Range(0, Count).Select(x => (GetKey(x), Index: x));

        await Parallel.ForEachAsync(keys,
            async (input, _) =>
            {
                var (key, i) = input;

                var id = generator(key);

                _res[i] = await id;
            }
        );

        return VerifyCorrectness();
    }

    private bool VerifyCorrectness()
    {
        return _res.All(x => x == Guid.Empty ? throw new Exception("Empty Guid") : true);
    }

    private TradeKey GetKey(int i)
    {
        return Key == KeyRandomness.Random
            ? CreateTradeKey()
            : _keys[i % _keys.Count];
    }
}

public enum KeyRandomness
{
    Random,
    Exist,
}

public enum StoresEnum
{
    RedisFsync1Sec,
    RedisFsyncAlways,
    Redis,
    Postgres,
    Lightning,
    FasterKV,
    FasterKVSerialiser,
    FasterKVNoCommit,
    FasterKVSpanByte,
    ConcurrentDictionary
}

public struct TradeKey
{
    private sealed class TradeKeyEqualityComparer : IEqualityComparer<TradeKey>
    {
        public bool Equals(TradeKey x, TradeKey y)
        {
            return x.TradeDate.Equals(y.TradeDate) && x.ExchangeLinkId == y.ExchangeLinkId && x.ExchangeTradeId == y.ExchangeTradeId;
        }

        public int GetHashCode(TradeKey obj)
        {
            return HashCode.Combine(obj.TradeDate, obj.ExchangeLinkId, obj.ExchangeTradeId);
        }
    }

    public static IEqualityComparer<TradeKey> TradeKeyComparer { get; } = new TradeKeyEqualityComparer();

    public DateOnly TradeDate { get; set; }
    public string ExchangeLinkId { get; set; }
    public string ExchangeTradeId { get; set; }

    public override string ToString()
    {
        return $"{TradeDate:yyyy-MM-dd}~{ExchangeLinkId}~{ExchangeTradeId}";
    }

    public TradeKey(string key)
    {
        var parts = key.Split('~');

        TradeDate = DateOnly.Parse(parts[0]);
        ExchangeLinkId = parts[1];
        ExchangeTradeId = parts[2];
    }

    public int SpanSize => 4 + ExchangeLinkId.Length + ExchangeTradeId.Length;

    public int Write(in Span<byte> span)
    {
        if(span.Length < SpanSize)
        {
            throw new Exception("Span too small");
        }

        var currentKeySpan = span;
        var bytesCount = 0;

        if (!BitConverter.TryWriteBytes(span, TradeDate.DayNumber))
        {
            throw new Exception("Error writing TradeDate  bytes");
        }

        bytesCount += 4;
        currentKeySpan = currentKeySpan.Slice(bytesCount);

        var bytes = Encoding.UTF8.GetBytes(ExchangeLinkId.AsSpan(), currentKeySpan);

        bytesCount += bytes;
        currentKeySpan = span.Slice(bytesCount);

        Encoding.UTF8.GetBytes(ExchangeTradeId.AsSpan(), currentKeySpan);
        bytesCount += bytes;

        return bytesCount;
    }
}
