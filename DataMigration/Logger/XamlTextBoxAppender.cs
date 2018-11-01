using System;
using System.Configuration;
using System.Windows.Controls;

namespace DataMigration.Logger
{
    public class XamlTextBoxAppender
    {
        public XamlTextBoxAppender(TextBox textBox)
        {
            TextBox = textBox;
        }

        public TextBox TextBox { get; set; }
        
        public void Append(XamlLogArg logArg)
        {
            if (TextBox == null || logArg == null)
            {
                return;
            }

            var log = GetFormattedLog(logArg);
            TextBox.AppendText(log);
            TextBox.ScrollToEnd();
        }

        private static string GetFormattedLog(XamlLogArg logArg)
        {
            var result = string.Empty;
            result += DateTime.Now.ToShortDateString();
            result += ConfigurationManager.AppSettings["separator"];
            result += DateTime.Now.ToLongTimeString();
            result += ConfigurationManager.AppSettings["lineSeparator"];
            result += logArg.Level;
            result += ConfigurationManager.AppSettings["levelMessageSeparator"];
            result += logArg.Message;
            result += "\n";
            return result;
        }
    }
}
