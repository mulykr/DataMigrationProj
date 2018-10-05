namespace DataMigration.Logger
{
    using Enums;

    delegate void MakeLog(string message, LogLevel level);

    interface ILogger
    {
        void Log(string message, LogLevel level);
    }
}
