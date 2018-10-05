using System.Collections.Generic;
using DataMigration.PostgresDB.Entities;

namespace DataMigration.PostgresDB
{
    internal interface IDataProvider
    {
        List<HistoricalOcrData> GetHistoricalOcrData(int? count = null, bool deleteAfterSelect = false);
    }
}
