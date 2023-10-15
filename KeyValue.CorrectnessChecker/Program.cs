// See https://aka.ms/new-console-template for more information

using CommandLine;
using Humanizer;
using NUlid;
using System.Diagnostics;

if (args.Length == 0)
{
    var options = new Options()
    {
        Store = StoresEnum.FasterKVSpanByte,
        Number = 100,
    };

    RunSingleDebugAlternateMode(options);
    
}

return Parser.Default.ParseArguments<Options>(args)
    .MapResult(
        o => Run(o),
        _ => 1
    );

static int Run(Options options)
{
    var store = Benchmarks.CreateStore(options.Store);

    if (options.Run == Mode.Run)
    {
        store.Cleanup();

        var sw = Stopwatch.StartNew();

        var keys = DeterministicKeyGenerator.Generate(options.Number);

        foreach (var key in keys)
        {
            store.GetOrCreateKey(key);
        }

        var format = $"Added {options.Number} items in {sw.Elapsed.Humanize()}";

        Console.WriteLine(format);

        Environment.FailFast(format);
    }
    else if (options.Run == Mode.Verify)
    {
        var starTime = DateTimeOffset.UtcNow;
        store.Recover();

        Console.WriteLine($"Verifying with {options.Number} items");

        var sw = Stopwatch.StartNew();

        var keys = DeterministicKeyGenerator.Generate(options.Number);

        foreach (var key in keys)
        {
            var guid = store.GetOrCreateKey(key);

            var ulid = new Ulid(guid);

            var keyTime = ulid.Time;

            if (keyTime > starTime)
            {
                throw new Exception($"Key [{ulid}] time [{keyTime}] is greater than start time [{starTime}]");
            }

            if (keyTime < options.MinTime)
            {
                throw new Exception(
                    $"Key [{ulid}] time [{keyTime}] is less than min time [{options.MinTime}]. Cleanup probably wasn't run."
                );
            }
        }

        Console.WriteLine($"Verified {options.Number} items in {sw.Elapsed.Humanize()}");
    }
    else
    {
        throw new Exception("Unknown mode");
    }

    return 0;
}

void RunSingleDebugAlternateMode(Options options1)
{
    var file = "lastRun.txt";

    var lastRun = "";

    if (File.Exists(file))
    {
        lastRun = File.ReadAllText(file);
    }

    if (lastRun == "")
    {
        File.WriteAllText(file, DateTimeOffset.UtcNow.ToString());

        options1.Run = Mode.Run;

        Run(options1);

        Environment.Exit(0);
    }
    else
    {
        var lastRunTime = DateTimeOffset.Parse(lastRun);
        File.Delete(file);

        options1.Run = Mode.Verify;
        options1.MinTime = lastRunTime;

        Run(options1);

        Environment.Exit(0);
    }
}


public class Options
{
    [Value(0, Required = true, HelpText = "Run or Verify Store")]
    public Mode Run { get; set; }

    [Value(1, Required = true, HelpText = "Store to run or verify")]
    public StoresEnum Store { get; set; }

    [Option('n', "number", Required = false, HelpText = "Number of items to add", Default = 1000)]
    public int Number { get; set; }

    [Option(
        'm',
        "min-time",
        Required = false,
        HelpText = "Minimum time to verify Ulids have been created after. Usually start of the `Run` time."
    )]
    public DateTimeOffset MinTime { get; set; }
}

public enum Mode
{
    Run,
    Verify
}
