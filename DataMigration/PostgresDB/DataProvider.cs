using System;
using System.Collections.Generic;
using System.Text;
using DataMigration.Logger;
using DataMigration.Logger.Enums;
using DataMigration.PostgresDB.Entities;
using Npgsql;

namespace DataMigration.PostgresDB
{
    public class DataProvider : ILogSupporting
    {
        private readonly string _connectionString;
        private NpgsqlConnection _connection;

        public DataProvider(string connectionString)
        {
            LogEventHappened?.Invoke($"Creating new DataProvider. Connection string: {connectionString}", LogLevel.Debug);
            _connectionString = connectionString;
        }

        public event MakeLog LogEventHappened;

        public NpgsqlConnection Connection
        {
            get
            {
                if (_connection != null)
                {
                    return _connection;
                }

                try
                {
                    _connection = new NpgsqlConnection(_connectionString);
                    _connection.Open();
                    return _connection;
                }
                catch
                {
                    LogEventHappened?.Invoke("Error occured during creating and opening connection", LogLevel.Error);
                    throw;
                }
            }
        }

        public NpgsqlConnection GetOpenedConnection()
        {
            LogEventHappened?.Invoke("Creating and opening new connection to Postgres Db...", LogLevel.Debug);
            try
            {
                var connection = new NpgsqlConnection(_connectionString);
                connection.Open();
                LogEventHappened?.Invoke("Success! Connection opened!", LogLevel.Debug);
                return connection;
            }
            catch
            {
                LogEventHappened?.Invoke("Cannot create or open connection", LogLevel.Error);
                throw;
            }
        }

        public List<HistoricalOcrData> GetHistoricalOcrData(int? count = null)
        {
            var result = new List<HistoricalOcrData>();
            if (count == 0)
                return result;

            if (count < 0)
                throw new ArgumentException($"Cannot select data in negative count : {count}");

            var commandText = "SELECT * FROM historical_ocr_data";
            if (count != null)
                commandText += $" LIMIT {count}";

            LogEventHappened?.Invoke($"Started getting historical ocr data. Count:  {(count == null ? "all" : count.ToString())}", LogLevel.Debug);
            try
            {
                using (var connection = GetOpenedConnection())
                {
                    using (var command = new NpgsqlCommand(commandText, connection))
                    {
                        LogEventHappened?.Invoke("Executing query...", LogLevel.Debug);
                        var reader = command.ExecuteReader();
                        LogEventHappened?.Invoke("Done!", LogLevel.Debug);
                        while (reader.Read())
                        {
                            result.Add(new HistoricalOcrData
                            {
                                TenantId = reader.GetInt32(0),
                                FullFilePath = reader.GetString(1),
                                StatusId = reader.GetInt32(2),
                                ErrorMessage = reader.GetValue(3)?.ToString(),
                                Data = reader.GetString(4),
                                CreatedAt = reader.GetValue(5),
                                UpdatedAt = reader.GetValue(6)
                            });
                        }
                    }
                }
            }
            catch
            {
                LogEventHappened?.Invoke("Error occured during selecting data", LogLevel.Error);
                throw;
            }

            LogEventHappened?.Invoke($"Data is successfully selected! Actual total count: {result.Count}", LogLevel.Debug);
            return result;
        }

        public int InsertHistoricalOcrData(HistoricalOcrData dataToInsert, NpgsqlConnection connection)
        {
            if (dataToInsert == null)
            {
                LogEventHappened?.Invoke("Data to insert cannot be null", LogLevel.Error);
                throw new ArgumentNullException(nameof(dataToInsert));
            }

            if (connection == null)
            {
                LogEventHappened?.Invoke("Connection cannot be null", LogLevel.Error);
                throw new ArgumentNullException(nameof(connection));
            }

            LogEventHappened?.Invoke("Inserting data...", LogLevel.Debug);
            var commandText = "INSERT INTO historical_ocr_data(tenantId, fullFilePath, statusId, errorMessage, data, createdAt, updatedAt) VALUES " +
                              $"({dataToInsert.TenantId}, '{dataToInsert.FullFilePath}', {dataToInsert.StatusId}, {(string.IsNullOrEmpty(dataToInsert.ErrorMessage) ? "null" : dataToInsert.ErrorMessage)}, '{NormalizeJsonData(dataToInsert.Data)}', '{dataToInsert.CreatedAt}', '{dataToInsert.UpdatedAt}')";
            var affected = 0;
            try
            {
                using (var command = new NpgsqlCommand(commandText, connection))
                {
                    affected = command.ExecuteNonQuery();
                    LogEventHappened?.Invoke($"Data is inserted! Success! Row(s) affected: {affected}", LogLevel.Debug);
                }
            }
            catch (Exception e)
            {
                LogEventHappened?.Invoke(e.Message, LogLevel.Error);
            }
            
            return affected;
        }
        
        public int DeleteData(IList<HistoricalOcrData> data)
        {
            if (data == null)
                throw new ArgumentNullException(nameof(data));
            if (data.Count == 0)
                return 0;

            LogEventHappened?.Invoke("Deleting data...", LogLevel.Debug);
            var query = GetQueryStringToRemoveData(data);
            var affected = 0;
            try
            {
                using (var command = new NpgsqlCommand(query, GetOpenedConnection()))
                {
                    affected = command.ExecuteNonQuery();
                    LogEventHappened?.Invoke($"Deleted! Affected row(s): {affected}", LogLevel.Debug);
                }
            }
            catch (Exception e)
            {
                LogEventHappened?.Invoke(e.Message, LogLevel.Error);
            }
            
            return affected;
        }

        private static string GetQueryStringToRemoveData(IEnumerable<HistoricalOcrData> toDelete)
        {
            var sb = new StringBuilder("DELETE FROM historical_ocr_data WHERE ");
            foreach (var item in toDelete)
            {
                sb.Append($"(tenantid = {item.TenantId} AND fullfilepath = '{item.FullFilePath}') OR ");
            }

            sb = sb.Remove(sb.Length - 4, 4);
            return sb.ToString();
        }

        private static string NormalizeJsonData(string data)
        {
            return data?.Replace('\'', '.');
        }
    }
}