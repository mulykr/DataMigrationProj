using DataMigration.Logger.Enums;

namespace DataMigration.Logger
{
    public class XamlLogArg
    {
        public string Message { get; set; }
        public LogLevel Level { get; set; }

        public XamlLogArg(string message, LogLevel level)
        {
            Message = message;
            Level = level;
        }
    }
}
