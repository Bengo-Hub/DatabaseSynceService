using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DatabaseSyncService
{
    public class DatabaseSync : IHostedService, IDisposable
    {
        private readonly ILogger<DatabaseSync> _logger;
        private readonly IConfiguration _configuration;
        private readonly List<string> _tablesToSync;
        private int _currentTableIndex = 0;

        public DatabaseSync(ILogger<DatabaseSync> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _tablesToSync = _configuration.GetSection("TablesToSync").Get<List<string>>();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Database Sync Service started.");

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await SyncDatabasesAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error during database synchronization.");
                }

                // Add a delay between sync cycles (e.g., 1 minute)
                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken);
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Database Sync Service stopped.");
            return Task.CompletedTask;
        }

        private async Task SyncDatabasesAsync(CancellationToken cancellationToken)
        {
            string progressFilePath = Path.Combine(Directory.GetCurrentDirectory(), "progress_report.csv");
            string errorFilePath = Path.Combine(Directory.GetCurrentDirectory(), "errors.txt");

            using (var progressWriter = new StreamWriter(progressFilePath, append: true))
            using (var errorWriter = new StreamWriter(errorFilePath, append: true))
            {
                if (new FileInfo(progressFilePath).Length == 0)
                {
                    progressWriter.WriteLine("Table,TotalRows,RowsPushed,SkippedDuplicates,Status,Timestamp");
                }

                try
                {
                    // Switch to the next table in the list
                    string table = _tablesToSync[_currentTableIndex];
                    _logger.LogInformation($"Syncing table: {table}");

                    // Open connections
                    using var mysqlConnection = new MySqlConnection(_configuration.GetConnectionString("MySQL"));
                    using var mssqlConnection = new SqlConnection(_configuration.GetConnectionString("MSSQL"));
                    await mysqlConnection.OpenAsync(cancellationToken);
                    await mssqlConnection.OpenAsync(cancellationToken);

                    // Sync the current table
                    var (rowsPushed, skippedDuplicates) = await SyncTableAsync(mysqlConnection, mssqlConnection, table, cancellationToken);

                    // Log progress
                    progressWriter.WriteLine($"{table},NA,{rowsPushed},{skippedDuplicates},Success,{DateTime.Now}");

                    // Move to the next table
                    _currentTableIndex = (_currentTableIndex + 1) % _tablesToSync.Count;
                }
                catch (Exception ex)
                {
                    errorWriter.WriteLine($"Error: {ex.Message} | Timestamp: {DateTime.Now}");
                    _logger.LogError(ex, "Error syncing databases.");
                }
            }
        }

        private async Task<(int rowsPushed, int skippedDuplicates)> SyncTableAsync(MySqlConnection mysqlConnection, SqlConnection mssqlConnection, string tableName, CancellationToken cancellationToken)
        {
            int rowsPushed = 0;
            int skippedDuplicates = 0;

            try
            {
                // Check if table exists in MS SQL, create if not
                if (!await TableExistsAsync(mssqlConnection, tableName, cancellationToken))
                {
                    await CreateMSSQLTableAsync(mysqlConnection, mssqlConnection, tableName, cancellationToken);
                }

                // Sync data in batches
                var offset = 0;
                var batchSize = 5000;

                while (true)
                {
                    var (rows, columns) = await FetchBatchAsync(mysqlConnection, tableName, offset, batchSize, cancellationToken);
                    if (rows.Count == 0) break;

                    foreach (var row in rows)
                    {
                        if (!await RowExistsAsync(mssqlConnection, tableName, columns, row, cancellationToken))
                        {
                            await InsertRowAsync(mssqlConnection, tableName, columns, row, cancellationToken);
                            await UpdateExportedStatusAsync(mysqlConnection, tableName, row, cancellationToken);
                            rowsPushed++;
                        }
                        else
                        {
                            await UpdateExportedStatusAsync(mysqlConnection, tableName, row, cancellationToken);
                            skippedDuplicates++;
                        }
                    }

                    offset += batchSize;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error syncing table: {tableName}");
                throw;
            }

            return (rowsPushed, skippedDuplicates);
        }

        private async Task<List<string>> GetMySQLTablesAsync(MySqlConnection connection, CancellationToken cancellationToken)
        {
            var tables = new List<string>();
            using var command = new MySqlCommand("SHOW TABLES", connection);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                tables.Add(reader.GetString(0));
            }
            return tables;
        }

        private async Task UpdateExportedStatusAsync(MySqlConnection mysqlConnection, string tableName, object[] row, CancellationToken cancellationToken)
        {
            string primaryKeyColumn = await GetPrimaryKeyColumnMySQlAsync(mysqlConnection, tableName, cancellationToken);
            if (string.IsNullOrEmpty(primaryKeyColumn))
            {
                _logger.LogWarning("No primary key column found for the table.");
                return;
            }

            List<string> columnNames = await GetColumnNamesAsync(mysqlConnection, tableName, cancellationToken);
            int primaryKeyIndex = columnNames.IndexOf(primaryKeyColumn);
            if (primaryKeyIndex == -1)
            {
                throw new InvalidOperationException($"Primary key column '{primaryKeyColumn}' not found in the row data.");
            }

            object primaryKeyValue = row[primaryKeyIndex];

            using var command = new MySqlCommand($"UPDATE {tableName} SET exported = 1000 WHERE {primaryKeyColumn} = @value", mysqlConnection);
            command.Parameters.AddWithValue("@value", primaryKeyValue);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task<List<string>> GetColumnNamesAsync(MySqlConnection connection, string tableName, CancellationToken cancellationToken)
        {
            var columns = new List<string>();
            using var command = new MySqlCommand($"SHOW COLUMNS FROM {tableName}", connection);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                columns.Add(reader.GetString(0));
            }
            return columns;
        }

        private async Task<bool> TableExistsAsync(SqlConnection connection, string tableName, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(
                $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'kenloadv2' AND TABLE_NAME = '{tableName}'",
                connection);
            var count = (int)await command.ExecuteScalarAsync(cancellationToken);
            return count > 0;
        }

        private async Task CreateMSSQLTableAsync(MySqlConnection mysqlConnection, SqlConnection mssqlConnection, string tableName, CancellationToken cancellationToken)
        {
            using var command = new MySqlCommand($"SHOW CREATE TABLE {tableName}", mysqlConnection);
            using var reader = await command.ExecuteReaderAsync(cancellationToken);

            if (await reader.ReadAsync(cancellationToken))
            {
                var createTableSql = reader.GetString(1);
                var sqlServerCreateTableSql = ConvertMySQLCreateTableToSQLServer(createTableSql, tableName);

                using var createCommand = new SqlCommand(sqlServerCreateTableSql, mssqlConnection);
                await createCommand.ExecuteNonQueryAsync(cancellationToken);

                _logger.LogInformation($"Table {tableName} created in MS SQL Server.");
            }
        }

        private string ConvertMySQLCreateTableToSQLServer(string mysqlCreateTableSql, string tableName)
        {
            mysqlCreateTableSql = mysqlCreateTableSql
                .Replace("`", "")
                .Replace("ENGINE=InnoDB", "")
                .Replace("AUTO_INCREMENT", "IDENTITY(1,1)");

            var columnDefinitions = mysqlCreateTableSql
                .Split(new[] { '(', ')' }, StringSplitOptions.RemoveEmptyEntries)[1]
                .Split(',')
                .Select(column => column.Trim())
                .ToList();

            for (int i = 0; i < columnDefinitions.Count; i++)
            {
                if (columnDefinitions[i].Contains("INT"))
                {
                    columnDefinitions[i] = columnDefinitions[i].Replace("INT", "INT");
                }
                else if (columnDefinitions[i].Contains("VARCHAR"))
                {
                    columnDefinitions[i] = columnDefinitions[i].Replace("VARCHAR", "NVARCHAR");
                }
                else if (columnDefinitions[i].Contains("DATETIME"))
                {
                    columnDefinitions[i] = columnDefinitions[i].Replace("DATETIME", "DATETIME2");
                }
                else if (columnDefinitions[i].Contains("TINYINT"))
                {
                    columnDefinitions[i] = columnDefinitions[i].Replace("TINYINT", "SMALLINT");
                }
            }

            var sqlServerCreateTableSql = $@"
        CREATE TABLE [kenloadv2].[{tableName}] (
            {string.Join(",\n    ", columnDefinitions)}
        );";

            return sqlServerCreateTableSql;
        }

        private async Task<(List<object[]> rows, List<string> columns)> FetchBatchAsync(MySqlConnection mysqlConnection, string tableName, int offset, int batchSize, CancellationToken cancellationToken)
        {
            var rows = new List<object[]>();
            var columns = new List<string>();

            using var command = new MySqlCommand($"SELECT * FROM {tableName} WHERE exported <> 1000 ORDER BY id DESC LIMIT @offset, @batchSize", mysqlConnection);
            command.Parameters.AddWithValue("@offset", offset);
            command.Parameters.AddWithValue("@batchSize", batchSize);

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new object[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.GetValue(i);
                    row[i] = value is DBNull ? null : value;
                }
                rows.Add(row);
            }

            for (int i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }

            return (rows, columns);
        }

        private async Task<string> GetPrimaryKeyColumnMySQlAsync(MySqlConnection connection, string tableName, CancellationToken cancellationToken)
        {
            using var command = new MySqlCommand(
                @"SELECT COLUMN_NAME
              FROM information_schema.COLUMNS
              WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = @tableName
                AND COLUMN_KEY = 'PRI'",
                connection);
            command.Parameters.AddWithValue("@tableName", tableName);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result?.ToString();
        }

        private async Task<bool> RowExistsAsync(SqlConnection connection, string tableName, List<string> columns, object[] row, CancellationToken cancellationToken)
        {
            string primaryKeyColumn = await GetPrimaryKeyColumnAsync(connection, tableName, cancellationToken);
            if (string.IsNullOrEmpty(primaryKeyColumn))
            {
                throw new InvalidOperationException($"Primary key column not found for table: {tableName}");
            }

            int primaryKeyIndex = columns.IndexOf(primaryKeyColumn);
            if (primaryKeyIndex == -1)
            {
                throw new InvalidOperationException($"Primary key column '{primaryKeyColumn}' not found in the row data.");
            }

            object primaryKeyValue = row[primaryKeyIndex];

            using var command = new SqlCommand($"SELECT COUNT(*) FROM [kenloadv2].[{tableName}] WHERE {primaryKeyColumn} = @value", connection);
            command.Parameters.AddWithValue("@value", primaryKeyValue);

            var count = (int)await command.ExecuteScalarAsync(cancellationToken);
            return count > 0;
        }

        private async Task InsertRowAsync(SqlConnection connection, string tableName, List<string> columns, object[] row, CancellationToken cancellationToken)
        {
            string primaryKeyColumn = await GetPrimaryKeyColumnAsync(connection, tableName, cancellationToken);
            var columnsToInsert = columns.Where(c => !c.Equals(primaryKeyColumn, StringComparison.OrdinalIgnoreCase)).ToList();
            var columnNames = string.Join(", ", columnsToInsert.Select(c => $"[{c}]"));
            var parameterNames = string.Join(", ", columnsToInsert.Select(c => $"@{c}"));

            using var command = new SqlCommand($"INSERT INTO [kenloadv2].[{tableName}] ({columnNames}) VALUES ({parameterNames})", connection);

            for (int i = 0; i < columns.Count; i++)
            {
                if (!columns[i].Equals(primaryKeyColumn, StringComparison.OrdinalIgnoreCase))
                {
                    var value = row[i];
                    command.Parameters.AddWithValue($"@{columns[i]}", value ?? DBNull.Value);
                }
            }

            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        private async Task<string> GetPrimaryKeyColumnAsync(SqlConnection connection, string tableName, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(
                @"SELECT COLUMN_NAME
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = 'kenloadv2'
            AND TABLE_NAME = @tableName
            AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1",
                connection);
            command.Parameters.AddWithValue("@tableName", tableName);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result?.ToString();
        }

        public void Dispose()
        {
            // Cleanup resources if needed
        }
    }
}