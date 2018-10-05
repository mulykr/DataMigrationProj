using System.Text;
using System.Collections.Generic;
using DataMigration.PostgresDB.Entities;
using Npgsql;

namespace DataMigration.PostgresDB
{
    using Logger;
    using Logger.Enums;

    partial class DataProvider : IDataProvider, ILogSupporting
    {
        private readonly string _connectionString;

        public DataProvider(string connectionString)
        {
            LogEventHappened?.Invoke($"Creating new DataProvider. Connection string: {connectionString}", LogLevel.Debug);
            _connectionString = connectionString;
            LogEventHappened?.Invoke($"Opening connection...", LogLevel.Debug);
            LogEventHappened?.Invoke($"Connection opened!", LogLevel.Debug);
        }

        public event MakeLog LogEventHappened;

        public  NpgsqlConnection GetOpenedConnection()
        {
            LogEventHappened("Creating and opening new connection to Postgres Db...", LogLevel.Debug);
            var connection = new NpgsqlConnection(_connectionString);
            connection.Open();
            LogEventHappened("Success! Connection opened!", LogLevel.Debug);
            return connection;
        }

        public List<HistoricalOcrData> GetHistoricalOcrData(int? count = null, bool deleteAfterSelect = false)
        {
            LogEventHappened?.Invoke($"Started getting historical ocr data. Count:  {(count == null ? "all" : count.ToString())}", LogLevel.Debug);

            string commandText = "SELECT * FROM historical_ocr_data";
            if (count != null)
            {
                commandText += string.Format(" LIMIT {0}", count);
            }

            List<HistoricalOcrData> result = new List<HistoricalOcrData>();

            using (var connection = GetOpenedConnection())
            using (var command = new NpgsqlCommand(commandText, connection))
            {
                LogEventHappened?.Invoke($"Executing query...", LogLevel.Debug);
                NpgsqlDataReader reader = command.ExecuteReader();
                LogEventHappened?.Invoke($"Done!", LogLevel.Debug);
                while (reader.Read())
                {
                    int tenantId = reader.GetInt32(0);
                    string fullFilePath = reader.GetString(1);
                    int statusId = reader.GetInt32(2);
                    string errorMessage = reader.GetValue(3)?.ToString();
                    string data = reader.GetString(4);
                    object createdAt = reader.GetValue(5);
                    object updatedAt = reader.GetValue(6);
                    result.Add(new HistoricalOcrData(tenantId, fullFilePath, statusId, errorMessage, data, createdAt, updatedAt));
                }
            }


            LogEventHappened?.Invoke($"Data is successfully selected! Actual total count: {result.Count}", LogLevel.Debug);
            if (deleteAfterSelect)
            {
                DeleteData(result);
            }

            return result;
        }

        public int InsertHistoricalOcrData(HistoricalOcrData dataToInsert)
        {
            LogEventHappened?.Invoke("Inserting data...", LogLevel.Debug);
            string commandText = string.Format("INSERT INTO historical_ocr_data(tenantId, fullFilePath, statusId, errorMessage, data, createdAt, updatedAt) VALUES ({0}, '{1}', {2}, {3}, '{4}', '{5}', '{6}')",
                                    dataToInsert.TenantId,
                                    dataToInsert.FullFilePath,
                                    dataToInsert.StatusId,
                                    string.IsNullOrEmpty(dataToInsert.ErrorMessage) ? "null" : dataToInsert.ErrorMessage,
                                    NormalizeJsonData(dataToInsert.Data),
                                    dataToInsert.CreatedAt,
                                    dataToInsert.UpdatedAt);


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

        private string GetInsertQueryString(IList<HistoricalOcrData> dataToInsert)
        {
            string commandText = "INSERT INTO historical_ocr_data VALUES ";
            StringBuilder sb = new StringBuilder(commandText);

            for (int i = 0; i < dataToInsert.Count - 1; i++)
            {
                sb.Append(string.Format("({0}, '{1}', {2}, {3}, '{4}', '{5}', '{6}'), ",
                                    dataToInsert[i].TenantId,
                                    dataToInsert[i].FullFilePath,
                                    dataToInsert[i].StatusId,
                                    string.IsNullOrEmpty(dataToInsert[i].ErrorMessage) ? "null" : dataToInsert[i].ErrorMessage,
                                    NormalizeJsonData(dataToInsert[i].Data),
                                    dataToInsert[i].CreatedAt,
                                    dataToInsert[i].UpdatedAt));
            }

            int lastIndex = dataToInsert.Count - 1;
            sb.Append(string.Format("({0}, '{1}', {2}, {3}, '{4}', '{5}', '{6}')",
                                    dataToInsert[lastIndex].TenantId,
                                    dataToInsert[lastIndex].FullFilePath,
                                    dataToInsert[lastIndex].StatusId,
                                    string.IsNullOrEmpty(dataToInsert[lastIndex].ErrorMessage) ? "null" : dataToInsert[lastIndex].ErrorMessage,
                                    NormalizeJsonData(dataToInsert[lastIndex].Data),
                                    dataToInsert[lastIndex].CreatedAt,
                                    dataToInsert[lastIndex].UpdatedAt));
            return sb.ToString();
        }

        private string GetQueryStringToRemoveData(IList<HistoricalOcrData> toDelete)
        {
            string commandText = "DELETE FROM historical_ocr_data WHERE ";
            StringBuilder sb = new StringBuilder(commandText);

            for (int i = 0; i < toDelete.Count - 1; i++)
            {
                sb.Append(string.Format("(tenantid = {0} AND fullfilepath = '{1}') OR ",
                                    toDelete[i].TenantId,
                                    toDelete[i].FullFilePath));
            }

            int lastIndex = toDelete.Count - 1;
            sb.Append(string.Format("(tenantid = {0} AND fullfilepath = '{1}')",
                                    toDelete[lastIndex].TenantId,
                                    toDelete[lastIndex].FullFilePath));
            return sb.ToString();
        }

        private string NormalizeJsonData(string data)
        {
            return data.Replace('\'', '.');
        }
    }
}
