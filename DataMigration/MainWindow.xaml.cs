using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Configuration;

namespace DataMigration
{
    using Logger;
    using Logger.Enums;
    using Models;
    using PostgresDB;
    using PostgresDB.Entities;
    using RabbitMQ.Client.Events;
    using Newtonsoft.Json;

    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private XamlTextBoxLogger _logger;
        private List<HistoricalOcrData> _recievedFromRabbit;

        public MainWindow()
        {
            InitializeComponent();
            _logger = new XamlTextBoxLogger(LogTextBox);
            _recievedFromRabbit = new List<HistoricalOcrData>();
            _logger.Log("=========== Logger started working! ==========", Logger.Enums.LogLevel.Info);
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            RabbitMqClient rabbitClient = new RabbitMqClient(ConfigurationManager.AppSettings["rabbitMqHost"], ConfigurationManager.AppSettings["rabbitVhost"]);
            rabbitClient.LogEventHappened += _logger.Log;

            try
            {
                _logger.Log("Starting process...", LogLevel.Info);

                DataProvider dataProvider = new DataProvider(ConfigurationManager.ConnectionStrings["psqlConnectionString"].ConnectionString);
                dataProvider.LogEventHappened += _logger.Log;

                int? amountToGet = 10;
                var data = dataProvider.GetHistoricalOcrData(amountToGet, true);


                _logger.Log($"Serializing data...", LogLevel.Debug);
                List<string> jsonParsedData = data.Select((item) => (JsonConvert.SerializeObject(item))).ToList();
                _logger.Log($"Serializing successed!", LogLevel.Debug);


                _logger.Log($"Sending {jsonParsedData.Count} serialized objects to RabbitMQ...", LogLevel.Debug);
                foreach (var jsonObj in jsonParsedData)
                {
                    rabbitClient.PublishMessageToQueue("data", jsonObj, "data");
                }
                _logger.Log($"Sending {jsonParsedData.Count} serialized objects to RabbitMQ successed!", LogLevel.Debug);


                _logger.Log($"Consuming messages from Rabbit", LogLevel.Debug);
                rabbitClient.ConsumeMessageFromQueue("data", ConsumerReceived);
                _logger.Log($"Consumed {_recievedFromRabbit.Count} message(s) from Rabbit!", LogLevel.Debug);

                
                DataProvider dataProviderDocker = new DataProvider(ConfigurationManager.ConnectionStrings["psqlDockerConnectionString"].ConnectionString);
                dataProviderDocker.LogEventHappened += _logger.Log;

                _logger.Log($"Inserting data into table in Docker's POSTGRES...", LogLevel.Debug);
                dataProviderDocker.InsertHistoricalOcrData(_recievedFromRabbit);
                _logger.Log($"Finished! Success!", LogLevel.Debug);

            }
            catch (Exception exc)
            {
                _logger.Log(exc.Message + "\n" + exc.StackTrace, LogLevel.Error);
            }
            finally
            {
                rabbitClient.Dispose();
                _recievedFromRabbit.Clear();
            }
        }

        private void ConsumerReceived(object sender, BasicDeliverEventArgs e)
        {
            var body = e.Body;
            var message = Encoding.UTF8.GetString(body);
            var histOcrData = JsonConvert.DeserializeObject<HistoricalOcrData>(message);
            _recievedFromRabbit.Add(histOcrData);
        }
    }
}