using System.Windows.Controls;
using DataMigration.Logger.Enums;

namespace DataMigration.Logger
{
    public class XamlTextBoxLogger : ILogger
    {
        private readonly XamlTextBoxAppender _appender;

        public XamlTextBoxLogger(TextBox textBox)
        {
            _appender = new XamlTextBoxAppender(textBox);
        }

        public void Log(string message, LogLevel level)
        {
            _appender.Append(new XamlLogArg(message, level));
        }
    }
}
