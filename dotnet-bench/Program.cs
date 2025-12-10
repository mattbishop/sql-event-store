using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Npgsql;
using NpgsqlTypes;
using Spectre.Console;
using System.Globalization;

internal class Program
{
    // Connection defaults (adjust if needed)
    private const string SqlServerConnectionString = "Server=localhost,1433;Database=eventstore;User Id=sa;Password=EventStore!2025;TrustServerCertificate=true;";
    private const string PostgresConnectionString = "Host=localhost;Port=5432;Database=eventstore;Username=postgres;Password=EventStore!2025;";
    private const string SqliteConnectionString = "Data Source=sqlite-store.db";
    private static readonly string Payload = BuildPayload();

    private static async Task Main(string[] args)
    {
        var (cliBackend, cliCount) = ParseArgs(args);

        AnsiConsole.MarkupLine("[bold]Event Store Benchmark[/]");

        var target = cliBackend ?? AnsiConsole.Prompt(
            new SelectionPrompt<string>()
                .Title("Choose backend")
                .AddChoices("T-SQL", "PostgreSQL", "SQLite"));

        var eventCount = cliCount ?? AnsiConsole.Prompt(
            new TextPrompt<int>("How many events?")
                .Validate(c => c > 0 ? ValidationResult.Success() : ValidationResult.Error("Must be > 0")));

        AnsiConsole.MarkupLine($"[grey]Running {eventCount} events on {target}...[/]");

        BenchmarkResult result = target switch
        {
            "T-SQL" => await RunSqlServer(eventCount),
            "PostgreSQL" => await RunPostgres(eventCount),
            "SQLite" => await RunSqlite(eventCount),
            _ => throw new InvalidOperationException()
        };

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("Backend");
        table.AddColumn("Inserted");
        table.AddColumn("Read Count");
        table.AddColumn("Insert ms");
        table.AddColumn("Read ms");

        table.AddRow(
            result.Backend,
            result.Inserted.ToString(),
            result.ReadCount.ToString(),
            result.InsertMs.ToString(),
            result.ReadMs.ToString());

        AnsiConsole.Write(table);
    }

    private static string NewEntityKey() => $"bench-{Guid.NewGuid():N}";

    private static async Task<BenchmarkResult> RunSqlServer(int count)
    {
        var entity = "bench";
        var entityKey = NewEntityKey();

        using var conn = new SqlConnection(SqlServerConnectionString);
        await conn.OpenAsync();

        var swInsert = Stopwatch.StartNew();
        Guid? previousId = null;

        for (int i = 0; i < count; i++)
        {
            using var cmd = new SqlCommand(
                "EXEC append_event @entity,@entity_key,@event,@data,@append_key,@previous_id,@timestamp,@event_id OUTPUT",
                conn);

            cmd.Parameters.AddWithValue("@entity", entity);
            cmd.Parameters.AddWithValue("@entity_key", entityKey);
            cmd.Parameters.AddWithValue("@event", "bench-event");
            cmd.Parameters.AddWithValue("@data", Payload);
            cmd.Parameters.AddWithValue("@append_key", Guid.NewGuid().ToString());
            cmd.Parameters.Add("@previous_id", SqlDbType.UniqueIdentifier).Value = (object?)previousId ?? DBNull.Value;
            cmd.Parameters.Add("@timestamp", SqlDbType.DateTimeOffset).Value = DBNull.Value;
            var outParam = cmd.Parameters.Add("@event_id", SqlDbType.UniqueIdentifier);
            outParam.Direction = ParameterDirection.Output;

            await cmd.ExecuteNonQueryAsync();
            previousId = outParam.Value is Guid g ? g : Guid.Parse(outParam.Value.ToString()!);
        }
        swInsert.Stop();

        var swRead = Stopwatch.StartNew();
        using (var readCmd = new SqlCommand(
                   "SELECT COUNT(*) FROM replay_events WHERE entity = @entity AND entity_key = @entity_key",
                   conn))
        {
            readCmd.Parameters.AddWithValue("@entity", entity);
            readCmd.Parameters.AddWithValue("@entity_key", entityKey);
            var readCountObj = await readCmd.ExecuteScalarAsync();
            var readCount = Convert.ToInt32(readCountObj);
            swRead.Stop();
            return new BenchmarkResult("T-SQL", count, swInsert.ElapsedMilliseconds, swRead.ElapsedMilliseconds, readCount);
        }
    }

    private static async Task<BenchmarkResult> RunPostgres(int count)
    {
        var entity = "bench";
        var entityKey = NewEntityKey();

        await using var conn = new NpgsqlConnection(PostgresConnectionString);
        await conn.OpenAsync();

        var swInsert = Stopwatch.StartNew();
        Guid? previousId = null;

        for (int i = 0; i < count; i++)
        {
            await using var cmd = new NpgsqlCommand(
                "SELECT append_event(@entity,@entity_key,@event,@data::jsonb,@append_key,@previous_id);",
                conn);
            cmd.Parameters.AddWithValue("entity", entity);
            cmd.Parameters.AddWithValue("entity_key", entityKey);
            cmd.Parameters.AddWithValue("event", "bench-event");
            cmd.Parameters.AddWithValue("data", Payload);
            cmd.Parameters.AddWithValue("append_key", Guid.NewGuid().ToString());
            cmd.Parameters.Add("previous_id", NpgsqlDbType.Uuid).Value = (object?)previousId ?? DBNull.Value;

            var result = await cmd.ExecuteScalarAsync();
            previousId = result is Guid g ? g : Guid.Parse(result!.ToString()!);
        }
        swInsert.Stop();

        var swRead = Stopwatch.StartNew();
        await using (var readCmd = new NpgsqlCommand(
                         "SELECT COUNT(*) FROM replay_events WHERE entity = @entity AND entity_key = @entity_key;",
                         conn))
        {
            readCmd.Parameters.AddWithValue("entity", entity);
            readCmd.Parameters.AddWithValue("entity_key", entityKey);
            var readCountObj = await readCmd.ExecuteScalarAsync();
            var readCount = Convert.ToInt32(readCountObj);
            swRead.Stop();
            return new BenchmarkResult("PostgreSQL", count, swInsert.ElapsedMilliseconds, swRead.ElapsedMilliseconds, readCount);
        }
    }

    private static async Task<BenchmarkResult> RunSqlite(int count)
    {
        var entity = "bench";
        var entityKey = NewEntityKey();

        await using var conn = new SqliteConnection(SqliteConnectionString);
        await conn.OpenAsync();

        var swInsert = Stopwatch.StartNew();
        string? previousId = null;

        for (int i = 0; i < count; i++)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
                VALUES ($entity, $entity_key, $event, $data, $append_key, $previous_id);
            ";
            cmd.Parameters.AddWithValue("$entity", entity);
            cmd.Parameters.AddWithValue("$entity_key", entityKey);
            cmd.Parameters.AddWithValue("$event", "bench-event");
            cmd.Parameters.AddWithValue("$data", Payload);
            cmd.Parameters.AddWithValue("$append_key", Guid.NewGuid().ToString());
            cmd.Parameters.AddWithValue("$previous_id", (object?)previousId ?? DBNull.Value);

            await cmd.ExecuteNonQueryAsync();

            await using var readIdCmd = conn.CreateCommand();
            readIdCmd.CommandText = @"
                SELECT event_id FROM ledger WHERE append_key = $append_key LIMIT 1;
            ";
            readIdCmd.Parameters.AddWithValue("$append_key", cmd.Parameters["$append_key"].Value);
            var newId = await readIdCmd.ExecuteScalarAsync();
            previousId = newId?.ToString();
        }
        swInsert.Stop();

        var swRead = Stopwatch.StartNew();
        await using (var readCmd = conn.CreateCommand())
        {
            readCmd.CommandText = @"
                SELECT COUNT(*) FROM replay_events WHERE entity = $entity AND entity_key = $entity_key;
            ";
            readCmd.Parameters.AddWithValue("$entity", entity);
            readCmd.Parameters.AddWithValue("$entity_key", entityKey);
            var readCountObj = await readCmd.ExecuteScalarAsync();
            var readCount = Convert.ToInt32(readCountObj);
            swRead.Stop();
            return new BenchmarkResult("SQLite", count, swInsert.ElapsedMilliseconds, swRead.ElapsedMilliseconds, readCount);
        }
    }

    private record BenchmarkResult(string Backend, int Inserted, long InsertMs, long ReadMs, int ReadCount);

    private static string BuildPayload()
    {
        var s1 = new string('A', 180);
        var s2 = new string('B', 160);
        var s3 = new string('C', 140);
        const decimal price = 12345.67m;
        const int quantity = 42;
        var priceStr = price.ToString(CultureInfo.InvariantCulture);
        return $"{{\"s1\":\"{s1}\",\"s2\":\"{s2}\",\"s3\":\"{s3}\",\"price\":{priceStr},\"qty\":{quantity}}}";
    }

    private static (string? Backend, int? Count) ParseArgs(string[] args)
    {
        string? backend = null;
        int? count = null;

        for (int i = 0; i < args.Length; i++)
        {
            var a = args[i];
            if (a.Equals("--backend", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                backend = args[++i].ToLowerInvariant() switch
                {
                    "tsql" or "t-sql" or "sqlserver" => "T-SQL",
                    "pg" or "postgres" or "postgresql" => "PostgreSQL",
                    "sqlite" => "SQLite",
                    _ => null
                };
            }
            else if (a.Equals("--count", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                if (int.TryParse(args[++i], out var c) && c > 0)
                    count = c;
            }
        }

        if (backend == null && count == null && args.Length > 0)
        {
            AnsiConsole.MarkupLine("[yellow]Unknown args ignored. Use --backend (tsql|postgres|sqlite) and --count N[/]");
        }

        return (backend, count);
    }
}
