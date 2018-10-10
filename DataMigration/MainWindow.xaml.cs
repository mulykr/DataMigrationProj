using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using DataMigration.Logger;
using DataMigration.Logger.Enums;
using DataMigration.Models;
using DataMigration.PostgresDB;
using DataMigration.PostgresDB.Entities;
using Newtonsoft.Json;
using RabbitMQ.Client.Events;

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
        private readonly RabbitMqProducer _rabbitProducer;
        private readonly RabbitMqConsumer _rabbitConsumer;


        public MainWindow()
        {
            try
            {
                InitializeComponent();
                _logger = new XamlTextBoxLogger(this);
                _logger.Log("=========== Logger started working! ==========", LogLevel.Info);
                _dataProvider = new DataProvider(ConfigurationManager.ConnectionStrings["psqlConnectionString"].ConnectionString);
                _dataProviderDocker = new DataProvider(ConfigurationManager.ConnectionStrings["psqlDockerConnectionString"].ConnectionString);

                _rabbitProducer = new RabbitMqProducer(ConfigurationManager.AppSettings["rabbitMqHost"],
                    ConfigurationManager.AppSettings["rabbitVhost"]);
                _rabbitConsumer = new RabbitMqConsumer(ConfigurationManager.AppSettings["rabbitMqHost"],
                    ConfigurationManager.AppSettings["rabbitVhost"]);
                _dataProvider.LogEventHappened += _logger.Log;
                _dataProviderDocker.LogEventHappened += _logger.Log;

                _rabbitProducer.LogEventHappened += _logger.Log;
                _rabbitConsumer.LogEventHappened += _logger.Log;
            }
            catch (Exception e)
            {
                _logger.Log($"Error occured while creating data providers: {e.Message}", LogLevel.Error);
            }
            
        }

        private async void StartMigratingDataClick(object sender, RoutedEventArgs e)
        {
            try
            {
                StartButton.IsEnabled = false;
                await Task.Run(() => StartMigration()).ContinueWith((obj) =>
                {
                    StartButton.Dispatcher.Invoke(() =>
                    {
                        return StartButton.IsEnabled = true;
                    });
                });
            }
            catch (Exception exception)
            {
                _logger.Log(exception.Message, LogLevel.Error);
                _logger.Log(exception.StackTrace, LogLevel.Error);
            }
        }

        private void StartMigration()
        {
            try
            {
                var queueName = ConfigurationManager.AppSettings["queueName"];
                var amountToGet = int.Parse(ConfigurationManager.AppSettings["postgresBatchSize"]);
                var data = _dataProvider.GetHistoricalOcrData(amountToGet);

                while (data.Count != 0)
                {
                    _logger.Log($"Processing data... Count: {data.Count}", LogLevel.Debug);

                    _logger.Log("Serializing data...", LogLevel.Debug);
                    var jsonParsedData = data.Select(item => (JsonConvert.SerializeObject(item))).ToList();
                    _logger.Log("Serializing successed!", LogLevel.Debug);
                    var sentData = SendDataToRabbitMq(jsonParsedData, _rabbitProducer, queueName);

                    _logger.Log($"Removing {sentData.Count} sent rows", LogLevel.Debug);

                    var dataToDelete = sentData.Select(JsonConvert.DeserializeObject<HistoricalOcrData>)
                        .ToList();
                    _dataProvider.DeleteData(dataToDelete);

                    data = _dataProvider.GetHistoricalOcrData(amountToGet);
                }


                _logger.Log("Consuming messages from Rabbit", LogLevel.Debug);
                _rabbitConsumer.ConsumeMessagesFromQueue(queueName, ReceiveMessage);
            }
            catch (Exception exc)
            {
                _logger.Log(exc.Message + "\n" + exc.StackTrace, LogLevel.Error);
            }
        }

        private List<string> SendDataToRabbitMq(IReadOnlyCollection<string> data, RabbitMqProducer producer, string queueName)
        {
            _logger.Log($"Sending {data.Count} serialized objects to RabbitMQ...", LogLevel.Debug);
            var sentMessages = (from msg in data let isSent = producer.PublishMessageToQueue(queueName, msg, queueName) where isSent select msg).ToList();

            _logger.Log($"{sentMessages.Count} serialized objects are sent to RabbitMQ successed!", LogLevel.Debug);
            return sentMessages;
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

        private void MainWindowOnClosing(object sender, CancelEventArgs e)
        {
            _rabbitConsumer?.Dispose();
            _rabbitProducer?.Dispose();
            MessageBox.Show("Bye!");
        }
    }
}