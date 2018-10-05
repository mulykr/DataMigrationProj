using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Configuration;
using DataMigration.Logger;
using DataMigration.Logger.Enums;
using DataMigration.Models;
using DataMigration.PostgresDB;
using DataMigration.PostgresDB.Entities;
using RabbitMQ.Client.Events;
using Newtonsoft.Json;

namespace DataMigration
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow
    {
        private readonly XamlTextBoxLogger _logger;
        private readonly List<HistoricalOcrData> _recievedFromRabbit;

        public MainWindow()
        {
            InitializeComponent();
            _logger = new XamlTextBoxLogger(LogTextBox);
            _recievedFromRabbit = new List<HistoricalOcrData>();
            _logger.Log("=========== Logger started working! ==========", LogLevel.Info);
        }

        private void StartMigratingDataClick(object sender, RoutedEventArgs e)
        {
            try
            {
                RabbitMqClient rabbitClient = new RabbitMqClient(ConfigurationManager.AppSettings["rabbitMqHost"], ConfigurationManager.AppSettings["rabbitVhost"]);
                rabbitClient.LogEventHappened += _logger.Log;

                DataProvider dataProvider = new DataProvider(ConfigurationManager.ConnectionStrings["psqlConnectionString"].ConnectionString);
                dataProvider.LogEventHappened += _logger.Log;

                DataProvider dataProviderDocker = new DataProvider(ConfigurationManager.ConnectionStrings["psqlDockerConnectionString"].ConnectionString);
                dataProviderDocker.LogEventHappened += _logger.Log;

                int? amountToGet = int.Parse(ConfigurationManager.AppSettings["objectsPerIteration"]);

                List<HistoricalOcrData> data = dataProvider.GetHistoricalOcrData(amountToGet, true);

                while (data.Count != 0)
                {
                    _logger.Log($"Processing data... Count: {data.Count}", LogLevel.Debug);

                    _logger.Log("Serializing data...", LogLevel.Debug);
                    List<string> jsonParsedData = data.Select((item) => (JsonConvert.SerializeObject(item))).ToList();
                    _logger.Log("Serializing successed!", LogLevel.Debug);


                    _logger.Log($"Sending {jsonParsedData.Count} serialized objects to RabbitMQ...", LogLevel.Debug);
                    foreach (var jsonObj in jsonParsedData)
                    {
                        rabbitClient.PublishMessageToQueue("data", jsonObj, "data");
                    }
                    _logger.Log($"Sending {jsonParsedData.Count} serialized objects to RabbitMQ successed!", LogLevel.Debug);


                    _logger.Log("Consuming messages from Rabbit", LogLevel.Debug);
                    rabbitClient.ConsumeMessageFromQueue("data", ConsumerReceived);
                    _logger.Log($"Consumed {_recievedFromRabbit.Count} message(s) from Rabbit!", LogLevel.Debug);

                    
                    _logger.Log("Inserting data into table in Docker\'s POSTGRES...", LogLevel.Debug);
                    dataProviderDocker.InsertHistoricalOcrData(_recievedFromRabbit);
                    _logger.Log("Finished iteration! Success!", LogLevel.Debug);

                    _recievedFromRabbit.Clear();
                    data = dataProvider.GetHistoricalOcrData(amountToGet, true);
                }

                _logger.Log("Data migration successed! Finished!", LogLevel.Debug);
            }
            catch (Exception exc)
            {
                _logger.Log(exc.Message + "\n" + exc.StackTrace, LogLevel.Error);
            }
            finally
            {
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