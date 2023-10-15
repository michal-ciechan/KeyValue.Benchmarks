using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Volumes;
using FluentMigrator;
using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using NUlid;
using Testcontainers.PostgreSql;

namespace KeyValue.Benchmarks.Stores;

public class PostgresStore : IStore
{
    private readonly PostgreSqlContainer _postgresContainer;
    private string _connectionString;
    private IVolume _volume;
    public int Port => _postgresContainer.GetMappedPublicPort(5432);

    public PostgresStore()
    {
        _volume = new VolumeBuilder()
            .WithCleanUp(true)
            .Build();

        _volume.CreateAsync().Wait();

        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:15.1")
            .WithVolumeMount(_volume, "/var/lib/postgresql/data")
            .WithCommand("-N 120") // Max Connections = 120
            .WithCleanUp(true)
            .Build();

        _postgresContainer.StartAsync().Wait();

        // connection string
        _connectionString = $"Host=localhost;Port={Port};Database=postgres;Username=postgres;Password=postgres;Max Auto Prepare=50";

        MigrateDb();
    }

    public void MigrateDb()
    {
        var provider = new ServiceCollection()
            // Add common FluentMigrator services
            .AddFluentMigratorCore()
            .ConfigureRunner(
                rb => rb
                    // Add SQLite support to FluentMigrator
                    .AddPostgres()
                    // Set the connection string
                    .WithGlobalConnectionString(
                        $"Host=localhost;Port={Port};Database=postgres;Username=postgres;Password=postgres"
                    )
                    // Define the assembly containing the migrations
                    .ScanIn(typeof(PostgresStore).Assembly)
                    .For.Migrations()
            )
            // Enable logging to console in the FluentMigrator way
            .AddLogging(lb => lb.AddFluentMigratorConsole())
            // Build the service provider
            .BuildServiceProvider(false);

        var runner = provider.GetRequiredService<IMigrationRunner>();


        // Execute the migrations
        runner.MigrateUp();
    }

    public Guid GetOrCreateKey(TradeKey key)
    {
        // Max Auto Prepare
        using (var connection = new NpgsqlConnection(_connectionString))
        {
            connection.Open();

            var sql = @"
INSERT INTO trade_keys (trade_date, exchange_link_id, exchange_trade_id, trade_id)
ON CONFLICT (trade_date, exchange_link_id, exchange_trade_id) DO NOTHING
RETURNING trade_id
";

            using (var command = new NpgsqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("tradeDate", key.TradeDate);
                command.Parameters.AddWithValue("exchangeLinkId", key.ExchangeLinkId);
                command.Parameters.AddWithValue("exchangeTradeId", key.ExchangeTradeId);
                command.Parameters.AddWithValue("tradeId", Ulid.NewUlid().ToGuidFast());

                using (var reader = command.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new Exception("No rows affected");
                    }

                    return reader.GetGuid(0);
                }
            }
        }
    }

    public async ValueTask<Guid> GetOrCreateKeyAsync(TradeKey key)
    {
        // Max Auto Prepare
        using (var connection = new NpgsqlConnection(_connectionString))
        {
            await connection.OpenAsync();

            var sql = @"
INSERT INTO trade_keys (trade_date, exchange_link_id, exchange_trade_id, trade_id)
VALUES (@tradeDate, @exchangeLinkId, @exchangeTradeId, @tradeId)
ON CONFLICT (trade_date, exchange_link_id, exchange_trade_id) DO UPDATE
SET    trade_date=EXCLUDED.trade_date
RETURNING trade_id
";

            using (var command = new NpgsqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("tradeDate", key.TradeDate);
                command.Parameters.AddWithValue("exchangeLinkId", key.ExchangeLinkId);
                command.Parameters.AddWithValue("exchangeTradeId", key.ExchangeTradeId);
                command.Parameters.AddWithValue("tradeId", Ulid.NewUlid().ToGuidFast());

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (!await reader.ReadAsync())
                    {
                        throw new Exception("No rows affected");
                    }

                    return reader.GetGuid(0);
                }
            }
        }
    }

    public void Cleanup()
    {
    }

    public void Recover()
    {
         throw new NotImplementedException("PostgresStore does not support recovery");
    }

    public void Dispose()
    {
        _postgresContainer?.DisposeAsync().GetAwaiter().GetResult();
        _volume?.DisposeAsync().GetAwaiter().GetResult();
    }
}

[Migration(20180430121800)]
public class AddLogTable : Migration
{
    public override void Up()
    {
        Create.Table("trade_keys")
            .WithColumn("trade_date").AsDate().PrimaryKey()
            .WithColumn("exchange_link_id").AsString().PrimaryKey()
            .WithColumn("exchange_trade_id").AsString().NotNullable().PrimaryKey()
            .WithColumn("trade_id").AsGuid();
    }

    public override void Down()
    {
        Delete.Table("TradeKeys");
    }
}
