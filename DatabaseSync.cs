using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
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
            try
            {
                // MySQL Connection
                using var mysqlConnection = new MySqlConnection(_configuration.GetConnectionString("MySQL"));
                await mysqlConnection.OpenAsync(cancellationToken);

                // MS SQL Connection
                using var mssqlConnection = new SqlConnection(_configuration.GetConnectionString("MSSQL"));
                await mssqlConnection.OpenAsync(cancellationToken);

                // Get MySQL tables
                var mysqlTables = await GetMySQLTablesAsync(mysqlConnection, cancellationToken);

                // Filter tables to sync based on the user-specified list
                var tablesToSync = mysqlTables.Intersect(_tablesToSync, StringComparer.OrdinalIgnoreCase).ToList();

                // Sync data for each table
                foreach (var table in tablesToSync)
                {
                    _logger.LogInformation($"Starting sync for table: {table}");

                    // Sync data from 2025 down to 2013
                    for (int year = 2025; year >= 2013; year--)
                    {
                        _logger.LogInformation($"Syncing data for year: {year}");

                        // Sync data for each month of the year
                        for (int month = 1; month <= 12; month++)
                        {
                            _logger.LogInformation($"Syncing data for month: {month}/{year}");

                            int totalRecords = 0;
                            int rowsSynced = 0;
                            int offset = 0;
                            const int batchSize = 1000;

                            do
                            {
                                // Fetch a batch of rows for the current month and year
                                var (rows, columns) = await FetchBatchByMonthAsync(mysqlConnection, table, year, month, offset, batchSize, cancellationToken);
                                totalRecords += rows.Count;

                                if (rows.Count == 0)
                                {
                                    break; // No more rows to process for this month
                                }

                                // Sync the batch
                                rowsSynced += await SyncBatchAsync(mssqlConnection, table, columns, rows, cancellationToken);

                                // Update the offset for the next batch
                                offset += batchSize;

                                _logger.LogInformation($"Synced {rowsSynced} rows out of {totalRecords} for {month}/{year} in table: {table}");

                            } while (!cancellationToken.IsCancellationRequested);

                            _logger.LogInformation($"Finished syncing data for {month}/{year} in table: {table}. Total records: {totalRecords}, Rows synced: {rowsSynced}");
                        }
                    }

                    _logger.LogInformation($"Finished sync for table: {table}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing databases.");
            }
        }

        private async Task<(List<object[]> rows, List<string> columns)> FetchBatchByMonthAsync(MySqlConnection mysqlConnection, string tableName, int year, int month, int offset, int batchSize, CancellationToken cancellationToken)
        {
            var rows = new List<object[]>();
            var columns = new List<string>();

            // Fetch data for the specific month and year
            using var command = new MySqlCommand(
                $"SELECT * FROM {tableName} WHERE YEAR(wbrg_ticket_date) = @year AND MONTH(wbrg_ticket_date) = @month ORDER BY wbrg_ticket_date DESC LIMIT @offset, @batchSize",
                mysqlConnection);
            command.Parameters.AddWithValue("@year", year);
            command.Parameters.AddWithValue("@month", month);
            command.Parameters.AddWithValue("@offset", offset);
            command.Parameters.AddWithValue("@batchSize", batchSize);

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new object[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                }
                rows.Add(row);
            }

            // Get column names
            for (int i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }

            return (rows, columns);
        }

        private async Task<int> SyncBatchAsync(SqlConnection mssqlConnection, string tableName, List<string> columns, List<object[]> rows, CancellationToken cancellationToken)
        {
            int rowsSynced = 0;

            foreach (var row in rows)
            {
                try
                {
                    if (!await RowExistsAsync(mssqlConnection, tableName, columns, row, cancellationToken))
                    {
                        await InsertRowAsync(mssqlConnection, tableName, columns, row, cancellationToken);
                        rowsSynced++;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error syncing row in table: {tableName}");
                }
            }

            return rowsSynced;
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
            if (primaryKeyValue.GetType() == typeof(ulong))
            {
                primaryKeyValue = Convert.ToInt64(primaryKeyValue);
            }

            using var command = new SqlCommand($"SELECT COUNT(*) FROM [kenloadv2].[{tableName}] WITH (NOLOCK) WHERE {primaryKeyColumn} = @value", connection);
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
                    var value = row[i] ?? GetDefaultValueForColumnType(await GetColumnDataTypeAsync(connection, tableName, columns[i], cancellationToken));
                    command.Parameters.AddWithValue($"@{columns[i]}", value);
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

        private async Task<string> GetColumnDataTypeAsync(SqlConnection connection, string tableName, string columnName, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(
                @"SELECT DATA_TYPE 
              FROM INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_NAME = @tableName 
                AND COLUMN_NAME = @columnName",
                connection);
            command.Parameters.AddWithValue("@tableName", tableName);
            command.Parameters.AddWithValue("@columnName", columnName);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result?.ToString();
        }

        private object GetDefaultValueForColumnType(string columnType)
        {
            switch (columnType.ToUpper())
            {
                case "INT":
                case "BIGINT":
                case "SMALLINT":
                case "TINYINT":
                    return 0;

                case "FLOAT":
                case "DECIMAL":
                case "NUMERIC":
                    return 0.0;

                case "DATE":
                case "DATETIME":
                case "DATETIME2":
                    return new DateTime(1900, 1, 1);

                case "BIT":
                    return false;

                case "VARCHAR":
                case "NVARCHAR":
                case "CHAR":
                case "NCHAR":
                case "TEXT":
                    return "x";

                default:
                    return "x";
            }
        }

        public void Dispose()
        {
            // Cleanup resources if needed
        }
    }
}