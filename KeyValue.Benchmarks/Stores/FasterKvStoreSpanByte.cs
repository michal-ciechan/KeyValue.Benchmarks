using System.Diagnostics;
using FASTER.core;

namespace KeyValue.Benchmarks.Stores;

public class FasterKvStoreSpanByte : IStore
{
    private readonly IDevice _log;
    private readonly FasterKVSettings<SpanByte, Guid> _settings;
    private readonly FasterKV<SpanByte, Guid> _store;
    private readonly Thread _thread;
    private readonly CancellationTokenSource _cts;
    private readonly bool _waitForCommit;

    public FasterKvStoreSpanByte(StoresEnum storesEnum)
    {
        _log = Devices.CreateLogDevice("hlog.log"); // backing storage device
        // _objlog = Devices.CreateLogDevice("hlog.obj.log");
        _settings = new FasterKVSettings<SpanByte, Guid>
        {
            LogDevice = _log,
            ObjectLogDevice = new NullDevice(),
            CheckpointDir = "Checkpoints",
        };

        _waitForCommit = storesEnum != StoresEnum.FasterKVNoCommit;

        _store = new FasterKV<SpanByte, Guid>(_settings);

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

        if (_waitForCommit)
        {
            _thread.Start();
        }

    }

    public Guid GetOrCreateKey(TradeKey key)
    {
        using var session = _store.For(TradeKeyFunctions.Instance).NewSession<TradeKeyFunctions>();

        var guid = Guid.Empty;

        Span<byte> keySpan = stackalloc byte[key.SpanSize];

        key.Write(keySpan);

        var keySpanByte = SpanByte.FromFixedSpan(keySpan);

        var result = session.RMW(ref keySpanByte, ref guid);

        if (result.IsPending)
        {
            session.CompletePending(wait: true, spinWaitForCommit: _waitForCommit);
        }

        return guid;
    }

    public sealed class TradeKeyFunctions : FunctionsBase<SpanByte, Guid, Guid, Guid, Empty>
    {
        public static readonly TradeKeyFunctions Instance = new TradeKeyFunctions();

        /// <inheritdoc />
        public override bool SingleWriter(
            ref SpanByte key,
            ref Guid input,
            ref Guid src,
            ref Guid dst,
            ref Guid output,
            ref UpsertInfo upsertInfo,
            WriteReason reason)
        {
            dst = output = src;
            return true;
        }

        /// <inheritdoc />
        public override bool ConcurrentWriter(
            ref SpanByte key,
            ref Guid input,
            ref Guid src,
            ref Guid dst,
            ref Guid output,
            ref UpsertInfo upsertInfo)
        {
            dst = output = src;
            return true;
        }

        /// <inheritdoc />
        public override bool InitialUpdater(
            ref SpanByte key,
            ref Guid input,
            ref Guid value,
            ref Guid output,
            ref RMWInfo rmwInfo)
        {
            value = output = Guid.NewGuid();
            return true;
        }

        /// <inheritdoc />
        public override bool CopyUpdater(
            ref SpanByte key,
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
            ref SpanByte key,
            ref Guid input,
            ref Guid value,
            ref Guid output,
            ref RMWInfo rmwInfo)
        {
            output = value;
            return true;
        }

        /// <inheritdoc />
        /// <remarks>Avoids the "value = default" for added tombstone record, which do not have space for the payload</remarks>
        public override bool SingleDeleter(ref SpanByte key, ref Guid value, ref DeleteInfo deleteInfo) => true;

        public override bool NeedInitialUpdate(ref SpanByte key, ref Guid input, ref Guid output, ref RMWInfo rmwInfo)
        {
            return base.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);
        }

        public override bool NeedCopyUpdate(ref SpanByte key, ref Guid input, ref Guid oldValue, ref Guid output, ref RMWInfo rmwInfo)
        {
            return base.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output, ref rmwInfo);
        }
    }

    public ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key)
    {
        return ValueTask.FromResult(GetOrCreateKey(key));
    }


    public void Dispose()
    {
        _cts.Cancel();

        _thread.Join();

        _store.Dispose();
        _settings.Dispose();
        _log.Dispose();
    }
}
