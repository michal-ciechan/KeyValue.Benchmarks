// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using Humanizer;
using KeyValue.Benchmarks.Stores;
using Perfolizer.Horology;

Console.WriteLine("Hello, World!");

var sw = Stopwatch.StartNew();

BenchmarkRunner.Run<Benchmarks>(new Config());
// BenchmarkRunner.Run<Benchmarks>(new CustomDebugConfig());

Console.WriteLine($"Done in {sw.Elapsed.Humanize()}");

class Config : ManualConfig
{
    public Config()
    {
        Add(DefaultConfig.Instance.GetExporters().ToArray());
        Add(DefaultConfig.Instance.GetLoggers().ToArray());
        Add(DefaultConfig.Instance.GetColumnProviders().ToArray());

        SummaryStyle = BenchmarkDotNet.Reports.SummaryStyle.Default.WithTimeUnit(TimeUnit.Millisecond);
    }
}

class CustomDebugConfig : ManualConfig, IConfig
{
    // DebuggableConfig
    IEnumerable<Job> IConfig.GetJobs() => (IEnumerable<Job>) new Job[1]
    {
        JobMode<Job>.Default.WithToolchain((IToolchain) new InProcessEmitToolchain(TimeSpan.FromHours(1.0), true))
    };

    public CustomDebugConfig()
    {
        Add(DefaultConfig.Instance.GetExporters().ToArray());
        Add(DefaultConfig.Instance.GetLoggers().ToArray());
        Add(DefaultConfig.Instance.GetColumnProviders().ToArray());

        AddFilter(
            new SimpleFilter(
                x =>
                {
                    var regex = @"\[ParallelIterationCount=(?<ParallelIterationCount>\d+), Store=(?<Store>\w+)\]";

                    var match = Regex.Match(x.Parameters.ValueInfo, regex);

                    // throw if not success
                    if (!match.Success)
                    {
                        throw new Exception(
                            $"Failed to parse {x.Parameters.ValueInfo}. " +
                            $"Example expected value is [ParallelIterationCount=1, Store=RedisFsync1Sec]"
                        );
                    }

                    var parallelIterationCount = int.Parse(match.Groups["ParallelIterationCount"].Value);
                    var store = Enum.Parse<StoresEnum>(match.Groups["Store"].Value);

                    // return store == Stores.Postgres && parallelIterationCount > 1;
                    // return store == StoresEnum.Lightning && parallelIterationCount == 1;
                    return true;
                }
            )
        );
    }
}


public class Benchmarks
{
    private List<TradeKey> _keys;
    private IStore _store;

    [Params(1, 10_000, 100_000)]
    // [Params(10_000)]
    // [Params(1)]
    public int ParallelIterationCount { get; set; }

    [ParamsAllValues]
    // [Params(StoresEnum.FasterKVSpanByte)]
    // [Params(StoresEnum.FasterKV, StoresEnum.FasterKVSpanByte)]
    public StoresEnum Store { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _keys = Enumerable.Range(1, 1000)
            .Select(
                x => new TradeKey
                {
                    TradeDate = new DateOnly(2021, 1, 1).AddDays(Random.Shared.Next(0, 365)),
                    ExchangeLinkId = Guid.NewGuid().ToString(),
                    ExchangeTradeId = Guid.NewGuid().ToString(),
                }
            )
            .ToList();

        _store = Store switch
        {
            StoresEnum.RedisFsync1Sec => new RedisTradeKeyStore(Store),
            StoresEnum.RedisFsyncAlways => new RedisTradeKeyStore(Store),
            StoresEnum.Redis => new RedisTradeKeyStore(Store),
            StoresEnum.Postgres => new PostgresStore(),
            StoresEnum.Lightning => new LightningLmdbStore(),
            StoresEnum.FasterKV => new FasterKvStore(Store),
            StoresEnum.FasterKVSerialiser => new FasterKvStore(Store),
            StoresEnum.FasterKVNoCommit => new FasterKvStore(Store),
            StoresEnum.FasterKVSpanByte => new FasterKvStoreSpanByte(Store),
            StoresEnum.ConcurrentDictionary => new ConcurrentDictionaryStore(),
            _ => throw new ArgumentOutOfRangeException()
        };

        // Verify Correctness
        var id1 = _store.GetOrCreateKeyAsync(_keys[0]);
        var id2 = _store.GetOrCreateKeyAsync(_keys[0]);

        if(id1.Result != id2.Result)
        {
            throw new Exception("Id1 != Id2");
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _store.Dispose();
    }

    [Benchmark]
    public bool ParallelAsync()
    {
        return RunParallelAsyncLoop(key => _store.GetOrCreateKeyAsync(key));
    }

    private bool RunParallelAsyncLoop(Func<TradeKey, ValueTask<Guid>> generator)
    {
        var tasks = Enumerable.Range(0, ParallelIterationCount)
            .Select(
                async i =>
                {
                    var key = _keys[i % _keys.Count];

                    var id = await generator(key);

                    return id;
                }
            )
            .ToList();

        var res = Task.WhenAll(tasks).GetAwaiter().GetResult();

        return res.All(x => x != Guid.Empty);
    }

    private bool RunParallelLoop(Func<TradeKey, Guid> generator)
    {
        var res = new Guid[ParallelIterationCount];

        Parallel.For(
            0,
            ParallelIterationCount,
            i =>
            {
                var key = _keys[i % _keys.Count];

                var id = generator(key);

                res[i] = id;
            }
        );

        return res.All(x => x != Guid.Empty);
    }
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
    public DateOnly TradeDate { get; set; }
    public string ExchangeLinkId { get; set; }
    public string ExchangeTradeId { get; set; }

    public override string ToString()
    {
        return $"{TradeDate:yyyy-MM-dd}-{ExchangeLinkId}-{ExchangeTradeId}";
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
