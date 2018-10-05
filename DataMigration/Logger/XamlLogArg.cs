namespace DataMigration.Logger
{
    using Enums;

    class XamlLogArg
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
