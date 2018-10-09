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
        private readonly DataProvider _dataProvider;
        private readonly DataProvider _dataProviderDocker;


        public MainWindow()
        {
            InitializeComponent();
            _logger = new XamlTextBoxLogger(this);
            _logger.Log("=========== Logger started working! ==========", LogLevel.Info);

            try
            {
                _dataProvider = new DataProvider(ConfigurationManager.ConnectionStrings["psqlConnectionString"].ConnectionString);
                _dataProviderDocker = new DataProvider(ConfigurationManager.ConnectionStrings["psqlDockerConnectionString"].ConnectionString);
            }
            catch (Exception e)
            {
                _logger.Log($"Error occured while creating data providers: {e.Message}", LogLevel.Error);
            }
            

            _dataProvider.LogEventHappened += _logger.Log;
            _dataProviderDocker.LogEventHappened += _logger.Log;

        }

        private void StartMigratingDataClick(object sender, RoutedEventArgs e)
        {
            try
            {
                var mqClient = new RabbitMqClient(ConfigurationManager.AppSettings["rabbitMqHost"], ConfigurationManager.AppSettings["rabbitVhost"]);
                mqClient.LogEventHappened += _logger.Log;

                var amountToGet = int.Parse(ConfigurationManager.AppSettings["objectsPerIteration"]);
                var data = _dataProvider.GetHistoricalOcrData(amountToGet);

                while (data.Count != 0)
                {
                    _logger.Log($"Processing data... Count: {data.Count}", LogLevel.Debug);

                    _logger.Log("Serializing data...", LogLevel.Debug);
                    var jsonParsedData = data.Select((item) => (JsonConvert.SerializeObject(item))).ToList();
                    _logger.Log("Serializing successed!", LogLevel.Debug);

                    SendDataToRabbitMq(jsonParsedData, mqClient);
                    _dataProvider.DeleteData(data);

                    data = _dataProvider.GetHistoricalOcrData(amountToGet);
                }

                _logger.Log("Consuming messages from Rabbit", LogLevel.Debug);
                mqClient.ConsumeMessagesFromQueue("data", ReceiveMessage);
            }
            catch (Exception exc)
            {
                _logger.Log(exc.Message + "\n" + exc.StackTrace, LogLevel.Error);
            }
        }

        private void SendDataToRabbitMq(IReadOnlyCollection<string> data, RabbitMqClient rabbitClient)
        {
            try
            {
                _logger.Log($"Sending {data.Count} serialized objects to RabbitMQ...", LogLevel.Debug);
                foreach (var msg in data)
                {
                    rabbitClient.PublishMessageToQueue("data", msg, "data");
                }

                _logger.Log($"{data.Count} serialized objects are sent to RabbitMQ successed!", LogLevel.Debug);
            }
            catch (Exception exception)
            {
                _logger.Log($"Cannot send data to rabbit. Error occured: {exception.Message}", LogLevel.Error);
            }
        }

        private void ReceiveMessage(object sender, BasicDeliverEventArgs e)
        {
            try
            {
                _logger.Log("Message recieved!", LogLevel.Debug);
                var body = e.Body;
                var message = Encoding.UTF8.GetString(body);
                var histOcrData = JsonConvert.DeserializeObject<HistoricalOcrData>(message);
                _logger.Log($"Inserting data into table in Docker\'s POSTGRES... FullFilePath: {histOcrData.FullFilePath}", LogLevel.Debug);
                var inserted = _dataProviderDocker.InsertHistoricalOcrData(histOcrData, _dataProviderDocker.Connection);
                _logger.Log($"Success! Inserted: {inserted}", LogLevel.Debug);
            }
            catch (Exception exception)
            {
                _logger.Log(exception.Message, LogLevel.Error);
            }

        }
    }
}