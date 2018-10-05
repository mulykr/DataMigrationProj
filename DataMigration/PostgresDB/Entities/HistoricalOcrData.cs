namespace DataMigration.PostgresDB.Entities
{

    public class HistoricalOcrData
    {
        public HistoricalOcrData()
        {

        }

        public HistoricalOcrData(int tenantId, string fullFilePath, int statusId, string errorMessage, string data, object createdAt, object updatedAt)
        {
            TenantId = tenantId;
            FullFilePath = fullFilePath;
            StatusId = statusId;
            ErrorMessage = errorMessage;
            Data = data;
            CreatedAt = createdAt;
            UpdatedAt = updatedAt;
        }

        public int TenantId { get; set; }
        public string FullFilePath { get; set; }
        public int StatusId { get; set; }
        public string ErrorMessage { get; set; }
        public string Data { get; set; }
        public object CreatedAt { get; set; }
        public object UpdatedAt { get; set; }
    }
}
