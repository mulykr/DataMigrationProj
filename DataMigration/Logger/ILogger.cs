using DataMigration.Logger.Enums;

namespace DataMigration.Logger
{
    interface ILogger
    {
        void Log(string message, LogLevel level);
    }
}
