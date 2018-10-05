using System;
using System.Configuration;
using System.Windows.Controls;

namespace DataMigration.Logger
{
    class XamlTextBoxAppender
    {
        private static object locker = new object();
        public XamlTextBoxAppender(TextBox textBox)
        {
            TextBox = textBox;
        }

        public TextBox TextBox { get; set; }


        public void Append(XamlLogArg logArg)
        {
            if (TextBox != null && logArg != null)
            {
                string log = GetFormattedLog(logArg);
                TextBox.AppendText(log);
            }
        }

        private string GetFormattedLog(XamlLogArg logArg)
        {
            string result = string.Empty;
            result += DateTime.Now.ToShortDateString();
            result += ConfigurationManager.AppSettings["separator"];
            result += DateTime.Now.ToLongTimeString();
            result += ConfigurationManager.AppSettings["ConfigurationManager.AppSettings"];
            result += logArg.Level;
            result += ConfigurationManager.AppSettings["levelMessageSeparator"];
            result += logArg.Message;
            result += "\n";
            return result;
        }
    }
}
