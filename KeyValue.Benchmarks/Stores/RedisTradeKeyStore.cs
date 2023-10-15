using DotNet.Testcontainers;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using NUlid;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace KeyValue.Benchmarks.Stores;

public class RedisTradeKeyStore : IStore
{
    private RedisContainer _redisContainer;
    private ConnectionMultiplexer _redis;

    public RedisTradeKeyStore(global::StoresEnum storesEnum)
    {
        TestcontainersSettings.Logger = ConsoleLogger.Instance;

        var nameSuffix = storesEnum switch
        {
            global::StoresEnum.RedisFsync1Sec => "_fsync_1sec",
            global::StoresEnum.RedisFsyncAlways => "_fsync_always",
            global::StoresEnum.Redis => "_default",
            _ => throw new NotImplementedException(),
        };

        var volume = new VolumeBuilder()
            .WithName($"redis_data_{nameSuffix}_{Guid.NewGuid()}")
            .Build();

        volume.CreateAsync().Wait();

        switch (storesEnum)
        {
            case global::StoresEnum.RedisFsync1Sec:
            {
                var redisFsyncImage = new ImageFromDockerfileBuilder()
                    .WithDockerfileDirectory(CommonDirectoryPath.GetSolutionDirectory(), "Docker/redis-fsync-1sec")
                    .WithDockerfile("Dockerfile")
                    .WithName("redis:fsync-1sec")
                    .WithDeleteIfExists(true)
                    .WithCleanUp(true)
                    .Build();

                redisFsyncImage.CreateAsync().Wait();

                _redisContainer = new RedisBuilder()
                    .WithImage(redisFsyncImage.FullName)
                    .WithVolumeMount(volume, "/data")
                    // .WithPortBinding("6379", "6379")
                    .Build();
                break;
            }
            case global::StoresEnum.RedisFsyncAlways:
            {
                var redisFsyncImage = new ImageFromDockerfileBuilder()
                    .WithDockerfileDirectory(CommonDirectoryPath.GetSolutionDirectory(), "Docker/redis-fsync-always")
                    .WithDockerfile("Dockerfile")
                    .WithName("redis:fsync-always")
                    .WithDeleteIfExists(true)
                    .WithCleanUp(true)
                    .Build();

                redisFsyncImage.CreateAsync().Wait();

                _redisContainer = new RedisBuilder()
                    .WithImage(redisFsyncImage.FullName)
                    .WithVolumeMount(volume, "/data")
                    // .WithPortBinding("6379", "6379")
                    .Build();
                break;
            }
            case global::StoresEnum.Redis:
                _redisContainer = new RedisBuilder()
                    .WithImage("redis:7.2.1")
                    .WithVolumeMount(volume, "/data")
                    // .WithPortBinding("6379", "6379")
                    .Build();
                break;
            default:
                throw new NotImplementedException();
        }

        _redisContainer.StartAsync().Wait();

        var connectionString = $"localhost:{Port},allowAdmin=true";

        _redis = ConnectionMultiplexer.Connect(connectionString);

    }

    private ushort Port => _redisContainer.GetMappedPublicPort("6379");


    public Guid GetOrCreateKey(TradeKey key)
    {
        var id = Ulid.NewUlid().ToGuidFast();

        var redisKey = key.ToString();
        var redisValue = id.ToString();

        var db = _redis.GetDatabase();

        var wasSet = db.StringSet(redisKey, redisValue, null, When.NotExists);

        if (!wasSet)
        {
            var existingId = db.StringGet(redisKey).ToString();

            id = Guid.Parse(existingId);
        }

        return id;
    }

    public async ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key)
    {
        var id = Ulid.NewUlid().ToGuidFast();

        var redisKey = key.ToString();
        var redisValue = id.ToString();

        var db = _redis.GetDatabase();

        var wasSet = await db.StringSetAsync(redisKey, redisValue, null, When.NotExists);

        if (!wasSet)
        {
            var existingId = (await db.StringGetAsync(redisKey)).ToString();

            id = Guid.Parse(existingId);
        }

        return id;
    }

    public void Cleanup()
    {
        _redis.GetServer("localhost", Port).FlushAllDatabases();
    }

    public void Recover()
    {
    }

    public void Dispose()
    {
        _redisContainer?.DisposeAsync().GetAwaiter().GetResult();
        _redis?.Dispose();
    }
}
