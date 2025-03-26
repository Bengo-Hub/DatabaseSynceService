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
                using var mysqlConnection = new MySqlConnection(_configuration.GetConnectionString("MySQL"));
                await mysqlConnection.OpenAsync(cancellationToken);

                using var mssqlConnection = new SqlConnection(_configuration.GetConnectionString("MSSQL"));
                await mssqlConnection.OpenAsync(cancellationToken);

                var mysqlTables = await GetMySQLTablesAsync(mysqlConnection, cancellationToken);
                var tablesToSync = mysqlTables.Intersect(_tablesToSync, StringComparer.OrdinalIgnoreCase).ToList();

                if (tablesToSync.Contains("weighbridgetransactions"))
                {
                    await SyncWeighbridgeTransactionsWithAxleWeightsAsync(mysqlConnection, mssqlConnection, cancellationToken);
                    tablesToSync.Remove("weighbridgetransactions");
                    tablesToSync.Remove("axleweights");
                }

                foreach (var table in tablesToSync)
                {
                    await SyncTableAsync(mysqlConnection, mssqlConnection, table, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error syncing databases.");
            }
        }

        private async Task SyncTableAsync(
            MySqlConnection mysqlConnection,
            SqlConnection mssqlConnection,
            string tableName,
            CancellationToken cancellationToken)
        {
            _logger.LogInformation($"Starting sync for table: {tableName}");

            int offset = 0;
            bool isTableFullyProcessed = false;

            while (!isTableFullyProcessed)
            {
                var (rowsPushed, skippedDuplicates) = await SyncTableBatchAsync(
                    mysqlConnection, mssqlConnection, tableName, offset, cancellationToken);

                if (rowsPushed == 0)
                {
                    isTableFullyProcessed = true;
                    _logger.LogInformation($"Table {tableName} fully processed.");
                }
                else
                {
                    offset += rowsPushed;
                    _logger.LogInformation($"Synced {rowsPushed} rows from table {tableName}. Skipped {skippedDuplicates} duplicates.");
                }
            }
        }

        private async Task<(int rowsPushed, int skippedDuplicates)> SyncTableBatchAsync(
            MySqlConnection mysqlConnection,
            SqlConnection mssqlConnection,
            string tableName,
            int offset,
            CancellationToken cancellationToken)
        {
            const int batchSize = 1;
            int rowsPushed = 0;
            int skippedDuplicates = 0;
            const int maxRetries = 3;
            const int retryDelay = 5000;
            int retryCount = 0;

            try
            {
                if (!await TableExistsAsync(mssqlConnection, tableName, cancellationToken))
                {
                    await CreateMSSQLTableAsync(mysqlConnection, mssqlConnection, tableName, cancellationToken);
                }

                var (rows, columns) = await FetchBatchAsync(mysqlConnection, tableName, offset, batchSize, cancellationToken);
                if (rows.Count == 0) return (0, 0);

                foreach (var row in rows)
                {
                    var convertedRow = ConvertMySqlRowToSqlServer(row, columns);

                    if (!await RowExistsAsync(mssqlConnection, tableName, columns, convertedRow, cancellationToken))
                    {
                        await InsertRowWithTypeConversionAsync(mssqlConnection, tableName, columns, convertedRow, cancellationToken);
                        await UpdateExportedStatusAsync(mysqlConnection, tableName, row, cancellationToken);
                        rowsPushed++;
                    }
                    else
                    {
                        await UpdateExportedStatusAsync(mysqlConnection, tableName, row, cancellationToken);
                        skippedDuplicates++;
                    }
                }
            }
            catch (SqlException ex) when (ex.Number == 1205)
            {
                retryCount++;
                if (retryCount >= maxRetries)
                {
                    _logger.LogError(ex, $"Deadlock occurred while syncing table: {tableName} after {maxRetries} retries.");
                    throw;
                }
                await Task.Delay(retryDelay, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error syncing batch for table: {tableName}.");
                throw;
            }

            return (rowsPushed, skippedDuplicates);
        }

        private async Task SyncWeighbridgeTransactionsWithAxleWeightsAsync(
            MySqlConnection mysqlConnection,
            SqlConnection mssqlConnection,
            CancellationToken cancellationToken)
        {
            const string parentTable = "weighbridgetransactions";
            const string childTable = "axleweights";

            _logger.LogInformation($"Starting sync for {parentTable} with related {childTable}");

            int offset = 0;
            bool isTableFullyProcessed = false;

            while (!isTableFullyProcessed)
            {
                var (parentRows, parentColumns) = await FetchBatchAsync(mysqlConnection, parentTable, offset, 1, cancellationToken);

                if (parentRows.Count == 0)
                {
                    isTableFullyProcessed = true;
                    _logger.LogInformation($"{parentTable} fully processed.");
                    break;
                }

                int rowsPushed = 0;
                int skippedDuplicates = 0;
                int totalAxleWeightsProcessed = 0;

                foreach (var parentRow in parentRows)
                {
                    var convertedParentRow = ConvertMySqlRowToSqlServer(parentRow, parentColumns);
                    var ticketNumberIndex = parentColumns.IndexOf("wbrg_ticket_no");

                    if (ticketNumberIndex == -1)
                    {
                        _logger.LogError("wbrg_ticket_no column not found in weighbridgetransactions");
                        continue;
                    }

                    var ticketNumber = convertedParentRow[ticketNumberIndex].ToString();
                    int? parentIdInTarget = null;

                    try
                    {
                        _logger.LogInformation($"Processing weighbridge transaction with ticket: {ticketNumber}");

                        // Handle parent weighbridge transaction
                        parentIdInTarget = await GetParentIdByTicketAsync(mssqlConnection, parentTable, ticketNumber, cancellationToken);

                        if (parentIdInTarget == null)
                        {
                            _logger.LogInformation($"No existing record found for ticket {ticketNumber}, attempting to insert");
                            parentIdInTarget = await InsertParentAndGetIdAsync(mssqlConnection, parentTable, parentColumns, convertedParentRow, cancellationToken);

                            if (parentIdInTarget == null)
                            {
                                _logger.LogError($"Failed to insert parent row with ticket {ticketNumber}");
                                continue;
                            }

                            rowsPushed++;
                            _logger.LogInformation($"Successfully inserted new weighbridge transaction with ticket {ticketNumber}, ID: {parentIdInTarget}");
                        }
                        else
                        {
                            skippedDuplicates++;
                            _logger.LogInformation($"Found existing weighbridge transaction with ticket {ticketNumber}, ID: {parentIdInTarget}");
                        }

                        // Process all related axleweights for this transaction
                        _logger.LogInformation($"Starting to sync axleweights for ticket {ticketNumber}");
                        var axleWeightsResult = await ProcessAxleWeightsForTransaction(
                            mysqlConnection,
                            mssqlConnection,
                            childTable,
                            ticketNumber,
                            parentIdInTarget.Value,
                            cancellationToken);

                        totalAxleWeightsProcessed += axleWeightsResult.TotalProcessed;
                        _logger.LogInformation($"Completed syncing {axleWeightsResult.TotalProcessed} axleweights for ticket {ticketNumber}. " +
                            $"Inserted: {axleWeightsResult.Inserted}, Updated: {axleWeightsResult.Updated}, Skipped: {axleWeightsResult.Skipped}");

                        // Update exported status for parent
                        await UpdateExportedStatusAsync(mysqlConnection, parentTable, parentRow, cancellationToken);
                        _logger.LogInformation($"Updated exported status for weighbridge transaction {ticketNumber}");
                    }
                    catch (SqlException ex) when (ex.Number == 2627) // Unique key violation
                    {
                        _logger.LogWarning($"Duplicate key violation for ticket {ticketNumber}, attempting recovery");
                        parentIdInTarget = await GetParentIdByTicketAsync(mssqlConnection, parentTable, ticketNumber, cancellationToken);

                        if (parentIdInTarget == null)
                        {
                            _logger.LogError($"Failed to recover from duplicate key violation for ticket {ticketNumber}");
                            continue;
                        }

                        skippedDuplicates++;
                        _logger.LogInformation($"Recovered existing weighbridge transaction ID: {parentIdInTarget} for ticket {ticketNumber}");

                        // Process axleweights even after duplicate key recovery
                        _logger.LogInformation($"Starting to sync axleweights for recovered ticket {ticketNumber}");
                        var axleWeightsResult = await ProcessAxleWeightsForTransaction(
                            mysqlConnection,
                            mssqlConnection,
                            childTable,
                            ticketNumber,
                            parentIdInTarget.Value,
                            cancellationToken);

                        totalAxleWeightsProcessed += axleWeightsResult.TotalProcessed;
                        _logger.LogInformation($"Completed syncing {axleWeightsResult.TotalProcessed} axleweights for recovered ticket {ticketNumber}. " +
                            $"Inserted: {axleWeightsResult.Inserted}, Updated: {axleWeightsResult.Updated}, Skipped: {axleWeightsResult.Skipped}");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Error processing weighbridge transaction with ticket {ticketNumber}");
                    }
                }

                offset += parentRows.Count;
                _logger.LogInformation($"Batch summary - Weighbridge transactions: Inserted {rowsPushed}, Skipped {skippedDuplicates}. " +
                    $"Total axleweights processed: {totalAxleWeightsProcessed}");
            }
        }

        private async Task<int?> GetParentIdByTicketAsync(
         SqlConnection connection,
         string tableName,
         string ticketNumber,
         CancellationToken cancellationToken)
        {
            try
            {
                using var command = new SqlCommand(
                    $"SELECT id FROM [kenloadv2].[{tableName}] WITH (NOLOCK) WHERE wbrg_ticket_no = @ticketNumber",
                    connection);

                command.Parameters.AddWithValue("@ticketNumber", ticketNumber);
                var result = await command.ExecuteScalarAsync(cancellationToken);
                return result == null || result == DBNull.Value ? null : Convert.ToInt32(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error getting ID for ticket {ticketNumber}");
                return null;
            }
        }


        private async Task<AxleWeightsSyncResult> ProcessAxleWeightsForTransaction(
            MySqlConnection mysqlConnection,
            SqlConnection mssqlConnection,
            string childTable,
            string ticketNumber,
            int parentIdInTarget,
            CancellationToken cancellationToken)
        {
            var result = new AxleWeightsSyncResult();
            var (childRows, childColumns) = await FetchAxleWeightsByTicketAsync(mysqlConnection, childTable, ticketNumber, cancellationToken);

            _logger.LogInformation($"Found {childRows.Count} axleweights for ticket {ticketNumber} in source database");

            var weighbridgeTransactionIdIndex = childColumns.IndexOf("weighbridgetransactionsid");
            if (weighbridgeTransactionIdIndex == -1)
            {
                _logger.LogError("weighbridgetransactionsid column not found in axleweights table");
                return result;
            }

            foreach (var childRow in childRows)
            {
                try
                {
                    var convertedChildRow = ConvertMySqlRowToSqlServer(childRow, childColumns);

                    // Ensure the foreign key points to the correct parent
                    convertedChildRow[weighbridgeTransactionIdIndex] = parentIdInTarget;
                    _logger.LogDebug($"Set weighbridgetransactionsid to {parentIdInTarget} for axleweight");

                    // Check if this axleweight exists in target (using ticket number + axle number as unique identifier)
                    bool childExists = await AxleWeightExistsAsync(mssqlConnection, childTable, convertedChildRow, childColumns, cancellationToken);

                    if (!childExists)
                    {
                        // Insert new axleweight
                        await InsertRowWithTypeConversionAsync(mssqlConnection, childTable, childColumns, convertedChildRow, cancellationToken);
                        result.Inserted++;
                        _logger.LogDebug($"Inserted new axleweight for ticket {ticketNumber}");
                    }
                    else
                    {
                        // Update existing axleweight to ensure correct foreign key relationship
                        await UpdateAxleWeightAsync(mssqlConnection, childTable, childColumns, convertedChildRow, cancellationToken);
                        result.Updated++;
                        _logger.LogDebug($"Updated existing axleweight for ticket {ticketNumber}");
                    }

                    // Update exported status in source
                    await UpdateExportedStatusAsync(mysqlConnection, childTable, childRow, cancellationToken);
                    result.TotalProcessed++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error processing axleweight for ticket {ticketNumber}");
                    result.Skipped++;
                }
            }

            return result;
        }

        private async Task UpdateAxleWeightAsync(
            SqlConnection connection,
            string tableName,
            List<string> columns,
            object[] row,
            CancellationToken cancellationToken)
        {
            try
            {
                // Get the primary key column
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

                // Build the UPDATE statement
                var setClauses = new List<string>();
                var parameters = new List<SqlParameter>();

                for (int i = 0; i < columns.Count; i++)
                {
                    if (columns[i].Equals(primaryKeyColumn, StringComparison.OrdinalIgnoreCase))
                        continue;

                    setClauses.Add($"[{columns[i]}] = @{columns[i]}");
                    parameters.Add(new SqlParameter($"@{columns[i]}", row[i] ?? DBNull.Value));
                }

                var sql = $@"
                    UPDATE [kenloadv2].[{tableName}] 
                    SET {string.Join(", ", setClauses)}
                    WHERE {primaryKeyColumn} = @primaryKey";

                parameters.Add(new SqlParameter("@primaryKey", primaryKeyValue));

                using var command = new SqlCommand(sql, connection);
                command.Parameters.AddRange(parameters.ToArray());

                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating row in {tableName}");
                throw;
            }
        }

        #region Database Operations
        private async Task<List<string>> GetMySQLTablesAsync(MySqlConnection connection, CancellationToken cancellationToken)
        {
            var tables = new List<string>();
            try
            {
                using var command = new MySqlCommand("SHOW TABLES", connection);
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    tables.Add(reader.GetString(0));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error fetching MySQL tables.");
            }
            return tables;
        }

        private async Task UpdateExportedStatusAsync(MySqlConnection mysqlConnection, string tableName, object[] row, CancellationToken cancellationToken)
        {
            try
            {
                string primaryKeyColumn = await GetPrimaryKeyColumnAsync(mysqlConnection, tableName, cancellationToken);
                if (string.IsNullOrEmpty(primaryKeyColumn))
                {
                    _logger.LogWarning($"No primary key column found for table: {tableName}");
                    return;
                }

                var columnNames = await GetColumnNamesAsync(mysqlConnection, tableName, cancellationToken);
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
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating exported status for table: {tableName}");
            }
        }

        private async Task<List<string>> GetColumnNamesAsync(MySqlConnection connection, string tableName, CancellationToken cancellationToken)
        {
            var columns = new List<string>();
            try
            {
                using var command = new MySqlCommand($"SHOW COLUMNS FROM {tableName}", connection);
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    columns.Add(reader.GetString(0));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error fetching column names for table: {tableName}");
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

        private async Task<(List<object[]> rows, List<string> columns)> FetchBatchAsync(
            MySqlConnection mysqlConnection,
            string tableName,
            int offset,
            int batchSize,
            CancellationToken cancellationToken)
        {
            var rows = new List<object[]>();
            var columns = new List<string>();

            using var command = new MySqlCommand(
                $"SELECT * FROM {tableName} WHERE id != 1000 ORDER BY id DESC LIMIT @offset, @batchSize",
                mysqlConnection);

            command.Parameters.AddWithValue("@offset", offset);
            command.Parameters.AddWithValue("@batchSize", batchSize);

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new object[reader.FieldCount];
                reader.GetValues(row);
                rows.Add(row);
            }

            for (int i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }

            return (rows, columns);
        }

        private async Task<(List<object[]> rows, List<string> columns)> FetchAxleWeightsByTicketAsync(
            MySqlConnection connection,
            string tableName,
            string ticketNumber,
            CancellationToken cancellationToken)
        {
            var rows = new List<object[]>();
            var columns = new List<string>();

            using var command = new MySqlCommand(
                $"SELECT * FROM {tableName} WHERE wbrg_ticket_no = @ticketNumber",
                connection);

            command.Parameters.AddWithValue("@ticketNumber", ticketNumber);

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new object[reader.FieldCount];
                reader.GetValues(row);
                rows.Add(row);
            }

            for (int i = 0; i < reader.FieldCount; i++)
            {
                columns.Add(reader.GetName(i));
            }

            return (rows, columns);
        }

        private async Task<bool> AxleWeightExistsAsync(
            SqlConnection connection,
            string tableName,
            object[] row,
            List<string> columns,
            CancellationToken cancellationToken)
        {
            var ticketNumberIndex = columns.IndexOf("wbrg_ticket_no");
            var axleNumberIndex = columns.IndexOf("axle_number");

            if (ticketNumberIndex == -1 || axleNumberIndex == -1)
            {
                throw new InvalidOperationException("Required columns not found in axleweights table");
            }

            var ticketNumber = row[ticketNumberIndex].ToString();
            var axleNumber = row[axleNumberIndex].ToString();

            using var command = new SqlCommand(
                $"SELECT COUNT(*) FROM [kenloadv2].[{tableName}] WHERE wbrg_ticket_no = @ticketNumber AND axle_number = @axleNumber",
                connection);

            command.Parameters.AddWithValue("@ticketNumber", ticketNumber);
            command.Parameters.AddWithValue("@axleNumber", axleNumber);

            return (int)await command.ExecuteScalarAsync(cancellationToken) > 0;
        }

        private async Task<string> GetPrimaryKeyColumnAsync(MySqlConnection connection, string tableName, CancellationToken cancellationToken)
        {
            try
            {
                using var command = new MySqlCommand(
                    @"SELECT COLUMN_NAME FROM information_schema.COLUMNS
                      WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = @tableName
                        AND COLUMN_KEY = 'PRI'",
                    connection);
                command.Parameters.AddWithValue("@tableName", tableName);
                return (await command.ExecuteScalarAsync(cancellationToken))?.ToString();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error retrieving primary key column for table: {tableName}");
                return null;
            }
        }

        private async Task<string> GetPrimaryKeyColumnAsync(SqlConnection connection, string tableName, CancellationToken cancellationToken)
        {
            using var command = new SqlCommand(
                @"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = 'kenloadv2'
                    AND TABLE_NAME = @tableName
                    AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1",
                connection);
            command.Parameters.AddWithValue("@tableName", tableName);
            return (await command.ExecuteScalarAsync(cancellationToken))?.ToString();
        }

        private async Task<bool> RowExistsAsync(
            SqlConnection connection,
            string tableName,
            List<string> columns,
            object[] row,
            CancellationToken cancellationToken)
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

            using var command = new SqlCommand(
                $"SELECT COUNT(*) FROM [kenloadv2].[{tableName}] WITH (NOLOCK) WHERE {primaryKeyColumn} = @value",
                connection);
            command.Parameters.AddWithValue("@value", primaryKeyValue);

            return (int)await command.ExecuteScalarAsync(cancellationToken) > 0;
        }

        private async Task<int?> InsertParentAndGetIdAsync(
            SqlConnection connection,
            string tableName,
            List<string> columns,
            object[] row,
            CancellationToken cancellationToken)
        {
            string primaryKeyColumn = await GetPrimaryKeyColumnAsync(connection, tableName, cancellationToken);
            if (string.IsNullOrEmpty(primaryKeyColumn))
            {
                throw new InvalidOperationException($"Primary key column not found for table: {tableName}");
            }

            var columnsToInsert = columns.Where(c => !c.Equals(primaryKeyColumn, StringComparison.OrdinalIgnoreCase)).ToList();
            var columnNames = string.Join(", ", columnsToInsert.Select(c => $"[{c}]"));
            var parameterNames = string.Join(", ", columnsToInsert.Select(c => $"@{c}"));

            var sql = $@"
                INSERT INTO [kenloadv2].[{tableName}] ({columnNames}) 
                OUTPUT INSERTED.id
                VALUES ({parameterNames})";

            using var command = new SqlCommand(sql, connection);

            for (int i = 0; i < columns.Count; i++)
            {
                if (!columns[i].Equals(primaryKeyColumn, StringComparison.OrdinalIgnoreCase))
                {
                    command.Parameters.AddWithValue($"@{columns[i]}", row[i] ?? DBNull.Value);
                }
            }

            try
            {
                var newId = await command.ExecuteScalarAsync(cancellationToken);
                return newId == null || newId == DBNull.Value ? null : Convert.ToInt32(newId);
            }
            catch (SqlException ex) when (ex.Number == 2627)
            {
                // Rethrow to be handled by the caller
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error inserting row into {tableName}");
                return null;
            }
        }

        private async Task InsertRowWithTypeConversionAsync(
            SqlConnection connection,
            string tableName,
            List<string> columns,
            object[] row,
            CancellationToken cancellationToken)
        {
            try
            {
                string primaryKeyColumn = await GetPrimaryKeyColumnAsync(connection, tableName, cancellationToken);
                var columnsToInsert = columns.Where(c => !c.Equals(primaryKeyColumn, StringComparison.OrdinalIgnoreCase)).ToList();
                var columnNames = string.Join(", ", columnsToInsert.Select(c => $"[{c}]"));
                var parameterNames = string.Join(", ", columnsToInsert.Select(c => $"@{c}"));

                using var command = new SqlCommand(
                    $"INSERT INTO [kenloadv2].[{tableName}] ({columnNames}) VALUES ({parameterNames})",
                    connection);

                for (int i = 0; i < columns.Count; i++)
                {
                    if (!columns[i].Equals(primaryKeyColumn, StringComparison.OrdinalIgnoreCase))
                    {
                        var value = row[i];
                        var columnName = columns[i];

                        if (value is string && columnName.ToUpper().Contains("VARCHAR"))
                        {
                            value = ConvertMySQLTypeToSQLServerType("VARCHAR") == "NVARCHAR" ? value.ToString() : value;
                        }
                        else if (value is DateTime && columnName.ToUpper().Contains("DATETIME"))
                        {
                            value = ConvertMySQLTypeToSQLServerType("DATETIME") == "DATETIME2" ? value : value;
                        }
                        else if (value is short && columnName.ToUpper().Contains("TINYINT"))
                        {
                            value = ConvertMySQLTypeToSQLServerType("TINYINT") == "SMALLINT" ? value : value;
                        }

                        command.Parameters.AddWithValue($"@{columns[i]}", value ?? DBNull.Value);
                    }
                }

                await command.ExecuteNonQueryAsync(cancellationToken);
                _logger.LogInformation($"Inserted row into {tableName}");
            }
            catch (SqlException ex) when (ex.Number == 2627)
            {
                _logger.LogWarning($"Skipping duplicate row in table: {tableName}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error inserting row into {tableName}");
                throw;
            }
        }
        #endregion

        #region Helper Methods
        private object[] ConvertMySqlRowToSqlServer(object[] row, List<string> columns)
        {
            var convertedRow = new object[row.Length];
            Array.Copy(row, convertedRow, row.Length);

            for (int i = 0; i < convertedRow.Length; i++)
            {
                if (convertedRow[i] == DBNull.Value || convertedRow[i] == null)
                {
                    continue;
                }

                if (convertedRow[i] is ulong ulongValue)
                {
                    convertedRow[i] = ulongValue > long.MaxValue ? Convert.ToDecimal(ulongValue) : Convert.ToInt64(ulongValue);
                }
                else if (convertedRow[i] is uint uintValue)
                {
                    convertedRow[i] = Convert.ToInt64(uintValue);
                }
                else if (convertedRow[i] is ushort ushortValue)
                {
                    convertedRow[i] = Convert.ToInt32(ushortValue);
                }
                else if (convertedRow[i] is sbyte sbyteValue)
                {
                    convertedRow[i] = Convert.ToInt16(sbyteValue);
                }
                else if (convertedRow[i] is bool boolValue)
                {
                    convertedRow[i] = boolValue ? 1 : 0;
                }
                else if (convertedRow[i] is DateTime dateTimeValue)
                {
                    if (dateTimeValue < new DateTime(1753, 1, 1))
                    {
                        convertedRow[i] = new DateTime(1753, 1, 1);
                    }
                    else if (dateTimeValue > new DateTime(9999, 12, 31))
                    {
                        convertedRow[i] = new DateTime(9999, 12, 31);
                    }
                }
            }
            return convertedRow;
        }

        private string ConvertMySQLTypeToSQLServerType(string mysqlType)
        {
            switch (mysqlType.ToUpper())
            {
                case "INT": return "INT";
                case "VARCHAR": return "NVARCHAR";
                case "DATETIME": return "DATETIME2";
                case "TINYINT": return "SMALLINT";
                default: return mysqlType;
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
                if (columnDefinitions[i].Contains("INT")) columnDefinitions[i] = columnDefinitions[i].Replace("INT", "INT");
                else if (columnDefinitions[i].Contains("VARCHAR")) columnDefinitions[i] = columnDefinitions[i].Replace("VARCHAR", "NVARCHAR");
                else if (columnDefinitions[i].Contains("DATETIME")) columnDefinitions[i] = columnDefinitions[i].Replace("DATETIME", "DATETIME2");
                else if (columnDefinitions[i].Contains("TINYINT")) columnDefinitions[i] = columnDefinitions[i].Replace("TINYINT", "SMALLINT");
            }

            return $@"
                CREATE TABLE [kenloadv2].[{tableName}] (
                    {string.Join(",\n    ", columnDefinitions)}
                );";
        }
        #endregion

        private class AxleWeightsSyncResult
        {
            public int TotalProcessed { get; set; }
            public int Inserted { get; set; }
            public int Updated { get; set; }
            public int Skipped { get; set; }
        }

        public void Dispose()
        {
            // Cleanup resources if needed
        }
    }
}