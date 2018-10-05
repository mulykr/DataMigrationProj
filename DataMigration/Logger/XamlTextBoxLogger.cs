using DataMigration.Logger.Enums;
using System.Windows.Controls;

namespace DataMigration.Logger
{
    class XamlTextBoxLogger : ILogger
    {
        private XamlTextBoxAppender _appender;

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
