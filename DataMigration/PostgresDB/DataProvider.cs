using System;
using System.Text;
using System.Collections.Generic;
using DataMigration.PostgresDB.Entities;
using DataMigration.Logger;
using DataMigration.Logger.Enums;
using Npgsql;

namespace DataMigration.PostgresDB
{
    public class DataProvider : IDataProvider, ILogSupporting
    {
        private readonly string _connectionString;

        public DataProvider(string connectionString)
        {
            LogEventHappened?.Invoke($"Creating new DataProvider. Connection string: {connectionString}", LogLevel.Debug);
            _connectionString = connectionString;
        }

        public event MakeLog LogEventHappened;

        public  NpgsqlConnection GetOpenedConnection()
        {
            LogEventHappened?.Invoke("Creating and opening new connection to Postgres Db...", LogLevel.Debug);
            var connection = new NpgsqlConnection(_connectionString);
            connection.Open();
            LogEventHappened?.Invoke("Success! Connection opened!", LogLevel.Debug);
            return connection;
        }

        public List<HistoricalOcrData> GetHistoricalOcrData(int? count = null, bool deleteAfterSelect = false)
        {
            if (count != null && count < 0)
            {
                throw new ArgumentException($"Cannot select data in count less than 0. Count: {count}");
            }

            string commandText = "SELECT * FROM historical_ocr_data";
            if (count != null)
            {
                commandText += $" LIMIT {count}";
            }

            LogEventHappened?.Invoke($"Started getting historical ocr data. Count:  {(count == null ? "all" : count.ToString())}", LogLevel.Debug);

            var result = new List<HistoricalOcrData>();
            if (count == 0)
            {
                return result;
            }

            using (var connection = GetOpenedConnection())
            {
                using (var command = new NpgsqlCommand(commandText, connection))
                {
                    LogEventHappened?.Invoke("Executing query...", LogLevel.Debug);
                    var reader = command.ExecuteReader();
                    LogEventHappened?.Invoke("Done!", LogLevel.Debug);
                    while (reader.Read())
                    {
                        int tenantId = reader.GetInt32(0);
                        string fullFilePath = reader.GetString(1);
                        int statusId = reader.GetInt32(2);
                        string errorMessage = reader.GetValue(3)?.ToString();
                        string data = reader.GetString(4);
                        object createdAt = reader.GetValue(5);
                        object updatedAt = reader.GetValue(6);
                        result.Add(new HistoricalOcrData(tenantId, fullFilePath, statusId, errorMessage, data,
                            createdAt, updatedAt));
                    }
                }
            }

            LogEventHappened?.Invoke($"Data is successfully selected! Actual total count: {result.Count}", LogLevel.Debug);
            if (deleteAfterSelect && result.Count != 0)
            {
                int affected = DeleteData(result);
                LogEventHappened?.Invoke($"Removed {affected} rows!", LogLevel.Debug);
            }

            return result;
        }

        public int InsertHistoricalOcrData(HistoricalOcrData dataToInsert)
        {
            LogEventHappened?.Invoke("Inserting data...", LogLevel.Debug);
            string commandText = $"INSERT INTO historical_ocr_data(tenantId, fullFilePath, statusId, errorMessage, data, createdAt, updatedAt) VALUES ({dataToInsert.TenantId}, '{dataToInsert.FullFilePath}', {dataToInsert.StatusId}, {(string.IsNullOrEmpty(dataToInsert.ErrorMessage) ? "null" : dataToInsert.ErrorMessage)}, '{NormalizeJsonData(dataToInsert.Data)}', '{dataToInsert.CreatedAt}', '{dataToInsert.UpdatedAt}')";
            
            int affected;
            using (var command = new NpgsqlCommand(commandText, GetOpenedConnection()))
            {
                affected = command.ExecuteNonQuery();
            }

            LogEventHappened?.Invoke($"Data is inserted! Success! Row(s) affected: {affected}", LogLevel.Debug);
            return affected;
        }

        public int InsertHistoricalOcrData(IList<HistoricalOcrData> dataToInsert)
        {
            LogEventHappened?.Invoke("Inserting data...", LogLevel.Debug);
            string commandText = GetInsertQueryString(dataToInsert);

            int affected;
            using (var command = new NpgsqlCommand(commandText, GetOpenedConnection()))
            {
                affected = command.ExecuteNonQuery();
            }

            LogEventHappened?.Invoke($"Data is inserted! Success! Row(s) affected: {affected}", LogLevel.Debug);
            return affected;
        }

        private int DeleteData(IList<HistoricalOcrData> data)
        {
            LogEventHappened?.Invoke("Deleting selected data...", LogLevel.Debug);
            string query = GetQueryStringToRemoveData(data);
            int affected;
            using (var command = new NpgsqlCommand(query, GetOpenedConnection()))
            {
                affected = command.ExecuteNonQuery();
                LogEventHappened?.Invoke($"Deleted! Affected row(s): {affected}", LogLevel.Debug);
            }
            return affected;
        }

        private static string GetInsertQueryString(IList<HistoricalOcrData> dataToInsert)
        {
            var commandText = "INSERT INTO historical_ocr_data VALUES ";
            var sb = new StringBuilder(commandText);

            foreach (var item in dataToInsert)
            {
                sb.Append($"({item.TenantId}, '{item.FullFilePath}', {item.StatusId}, {(string.IsNullOrEmpty(item.ErrorMessage) ? "null" : item.ErrorMessage)}, '{NormalizeJsonData(item.Data)}', '{item.CreatedAt}', '{item.UpdatedAt}'), ");
            }

            sb = sb.Remove(sb.Length - 2, 2);
            return sb.ToString();
        }

        private static string GetQueryStringToRemoveData(IList<HistoricalOcrData> toDelete)
        {
            const string commandText = "DELETE FROM historical_ocr_data WHERE ";
            var sb = new StringBuilder(commandText);

            foreach (var item in toDelete)
            {
                sb.Append($"(tenantid = {item.TenantId} AND fullfilepath = '{item.FullFilePath}') OR ");
            }

            sb = sb.Remove(sb.Length - 4, 4);
            return sb.ToString();
        }

        private static string NormalizeJsonData(string data)
        {
            return data.Replace('\'', '.');
        }
    }
}
