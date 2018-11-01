using System.Threading.Tasks;
using DataMigration.Logger.Enums;

namespace DataMigration.Logger
{
    public class XamlTextBoxLogger
    {
        private readonly XamlTextBoxAppender _appender;
        private readonly MainWindow _window;

        public XamlTextBoxLogger(MainWindow window)
        {
            _window = window;
            _appender = new XamlTextBoxAppender(_window.LogTextBox);
        }

        public async void Log(string message, LogLevel level)
        {
            await Task.Run(() => _window.Dispatcher.Invoke(() => _appender.Append(new XamlLogArg(message, level))));
        }
    }
}
