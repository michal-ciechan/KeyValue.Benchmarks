using System.Diagnostics;
using System.Text;
using FASTER.core;
using Humanizer;
using LightningDB;
using NUlid;

namespace KeyValue.Benchmarks.Stores;

public class FasterKvStore : IStore
{
    private readonly IDevice _log;
    private readonly FasterKVSettings<TradeKey, Guid> _settings;
    private readonly FasterKV<TradeKey, Guid> _store;
    private readonly IDevice _objlog;
    private readonly Thread _thread;
    private readonly CancellationTokenSource _cts;
    private readonly bool _waitForCommit;

    public FasterKvStore(StoresEnum storesEnum)
    {
        _log = Devices.CreateLogDevice("hlog.log"); // backing storage device
        _objlog = Devices.CreateLogDevice("hlog.obj.log");
        _settings = new FasterKVSettings<TradeKey, Guid>
        {
            LogDevice = _log,
            ObjectLogDevice = _objlog,
            CheckpointDir = "Checkpoints",
        };

        if (storesEnum != StoresEnum.FasterKV)
        {
            _settings.KeySerializer = () => new TradeKeySerializer();
            _settings.ValueSerializer = () => new GuidSerializer();
        }

        _waitForCommit = storesEnum != StoresEnum.FasterKVNoCommit;

        _store = new FasterKV<TradeKey, Guid>(_settings);

        _cts = new CancellationTokenSource();

        _thread = new Thread(
            () =>
            {
                var sw = Stopwatch.StartNew();
                while (!_cts.IsCancellationRequested)
                {
                    sw.Restart();

                    _store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver, tryIncremental: true)
                        .GetAwaiter()
                        .GetResult();

                    var elapsedMs = sw.ElapsedMilliseconds;
                    var remaining = 5 - elapsedMs;

                    // Console.WriteLine($"Checkpoint took: {elapsedMs:N0}ms");

                    if (remaining > 0)
                    {
                        Thread.Sleep((int)remaining);
                    }
                }
            }
        );

        _thread.Start();
    }

    public Guid GetOrCreateKey(TradeKey key)
    {
        throw new NotImplementedException();
    }

    public sealed class TradeKeyFunctions : SimpleFunctions<TradeKey, Guid, Guid>
    {
        public static readonly TradeKeyFunctions Instance = new TradeKeyFunctions();

        /// <inheritdoc />
        public override bool CopyUpdater(
            ref TradeKey key,
            ref Guid input,
            ref Guid oldValue,
            ref Guid newValue,
            ref Guid output,
            ref RMWInfo rmwInfo)
        {
            newValue = output = oldValue;
            return true;
        }

        /// <inheritdoc />
        public override bool InPlaceUpdater(
            ref TradeKey key,
            ref Guid input,
            ref Guid value,
            ref Guid output,
            ref RMWInfo rmwInfo
            )
        {
            output = value;
            return true;
        }

        /// <inheritdoc />
        public override bool InitialUpdater(
            ref TradeKey key,
            ref Guid input,
            ref Guid value,
            ref Guid output,
            ref RMWInfo rmwInfo
        )
        {
            value = output = Ulid.NewUlid().ToGuidFast();
            return true;
        }
    }

    public async ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key)
    {
        using var session = _store.For(TradeKeyFunctions.Instance).NewSession<TradeKeyFunctions>();

        var guid = Guid.Empty;

        var result = await session.RMWAsync(ref key, ref guid);

        while (result.Status.IsPending)
            result = await result.CompleteAsync();

        if(!_waitForCommit)
            return guid;

        await session.WaitForCommitAsync();

        return guid;
    }

    public void Cleanup()
    {
        if (Directory.Exists(_settings.CheckpointDir))
        {
            Directory.Delete(_settings.CheckpointDir, recursive: true);
        }
    }

    public void Recover()
    {
        var sw = Stopwatch.StartNew();

        var recoveryVer = _store.Recover();

        Console.WriteLine($"Recovered to version {recoveryVer} in {sw.Elapsed.Humanize()}");
    }


    public void Dispose()
    {
        _cts.Cancel();

        _thread.Join();

        _store.Dispose();
        _settings.Dispose();
        _objlog.Dispose();
        _log.Dispose();
    }
}


public sealed class TradeKeySerializer : IObjectSerializer<TradeKey>
{
    private Stream _stream = null!;

    public void BeginSerialize(Stream stream)
    {
        _stream = stream;
    }

    public void Serialize(ref TradeKey key)
    {
        Span<byte> keyBytes = stackalloc byte[100];
        var currentKeySpan = keyBytes;
        var bytesCount = 0;

        if (!BitConverter.TryWriteBytes(keyBytes, key.TradeDate.DayNumber))
        {
            throw new Exception("Error writing TradeDate  bytes");
        }

        bytesCount += 4;

        currentKeySpan = currentKeySpan.Slice(bytesCount);

        var bytes = Encoding.UTF8.GetBytes(key.ExchangeLinkId.AsSpan(), currentKeySpan.Slice(1));
        currentKeySpan[0] = (byte)bytes;
        bytesCount += bytes + 1;

        currentKeySpan = keyBytes.Slice(bytesCount);

        bytes = Encoding.UTF8.GetBytes(key.ExchangeTradeId.AsSpan(), currentKeySpan.Slice(1));
        currentKeySpan[0] = (byte)bytes;
        bytesCount += bytes + 1;

        keyBytes = keyBytes.Slice(0, bytesCount);

        Span<byte> bytesCountBytes = stackalloc byte[4];
        BitConverter.TryWriteBytes(bytesCountBytes, bytesCount);

        _stream.Write(bytesCountBytes);
        _stream.Write(keyBytes);
    }

    public void EndSerialize()
    {
        _stream = null!;
    }

    public void BeginDeserialize(Stream stream)
    {
        _stream = stream;
    }

    public void Deserialize(out TradeKey obj)
    {
        Span<byte> keyBytes = stackalloc byte[4];

        var buffer = keyBytes.Slice(0, 4);

        _stream.ReadExactly(buffer);

        var bytesCount = BitConverter.ToInt32(keyBytes.Slice(0,4));

        Span<byte> objBytes = stackalloc byte[bytesCount];

        _stream.ReadExactly(objBytes);

        var bytesRead = 0;

        var dayNumber = BitConverter.ToInt32(objBytes.Slice(0, 4));
        bytesRead += 4;

        var exchangeLinkIdLength = objBytes[bytesRead++];
        var exchangeLinkId = Encoding.UTF8.GetString(objBytes.Slice(bytesRead, exchangeLinkIdLength));
        bytesRead += exchangeLinkIdLength;

        var exchangeTradeIdLength = objBytes[bytesRead++];
        var exchangeTradeId = Encoding.UTF8.GetString(objBytes.Slice(bytesRead, exchangeTradeIdLength));


        obj = new TradeKey
        {
            TradeDate = DateOnly.FromDayNumber(dayNumber),
            ExchangeLinkId = exchangeLinkId,
            ExchangeTradeId = exchangeTradeId,
        };
    }

    public void EndDeserialize()
    {
        _stream = null!;
    }
}

public sealed class GuidSerializer : IObjectSerializer<Guid>
{
    private Stream _stream;

    public void BeginSerialize(Stream stream)
    {
        _stream = stream;
    }

    public void Serialize(ref Guid obj)
    {
        Span<byte> bytes = stackalloc byte[16];
        obj.TryWriteBytes(bytes);
        _stream.Write(bytes);
    }

    public void EndSerialize()
    {
        _stream = null!;
    }

    public void BeginDeserialize(Stream stream)
    {
        _stream = stream;
    }

    public void Deserialize(out Guid obj)
    {
        Span<byte> bytes = stackalloc byte[16];
        _stream.ReadExactly(bytes);
        obj = new Guid(bytes);
    }

    public void EndDeserialize()
    {
        _stream = null!;
    }
}
