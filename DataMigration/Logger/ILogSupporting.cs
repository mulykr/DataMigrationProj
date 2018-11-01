using DataMigration.Logger.Enums;

namespace DataMigration.Logger
{
    public delegate void MakeLog(string message, LogLevel level);
    public interface ILogSupporting
    {
        event MakeLog LogEventHappened;
    }
}
