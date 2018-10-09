using System;
using System.Text;
using DataMigration.Logger;
using DataMigration.Logger.Enums;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DataMigration.Models
{
    public class RabbitMqClient : ILogSupporting
    {
        private readonly ConnectionFactory _connectionFactory;
        private IConnection _connection;
        private IModel _channel;
        
        public RabbitMqClient(string host, string vhost)
        {
            _connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(host),
                VirtualHost = vhost
            };
        }

        public event MakeLog LogEventHappened;

        public IConnection Connection => _connection ?? (_connection = _connectionFactory.CreateConnection());

        public IModel Channel => _channel ?? (_channel = Connection.CreateModel());
        
        public void PublishMessageToQueue(string queueName, string message, string routingKey)
        {
            try
            {
                using (var connection = _connectionFactory.CreateConnection())
                {
                    using (var channel = connection.CreateModel())
                    {
                        CreateQueue(channel, queueName);

                        var body = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish(exchange: string.Empty,
                            routingKey: routingKey,
                            basicProperties: null,
                            body: body);
                    }
                }
            }
            catch (Exception exception)
            {
                LogEventHappened?.Invoke($"Error occured while sending message: {exception.Message}", LogLevel.Error);
            }
        }

        public void ConsumeMessagesFromQueue(string queueName, EventHandler<BasicDeliverEventArgs> handler)
        {
            try
            {
                DebugLog("Consuming message from queue...");
                CreateQueue(Channel, queueName);

                var consumer = new EventingBasicConsumer(Channel);
                consumer.Received += handler;
                DebugLog("Pending to be consumed: " + Channel.MessageCount("data"));
                Channel.BasicConsume(queue: queueName,
                    autoAck: true,
                    consumer: consumer);
                DebugLog("Consuming... Messages count: " + Channel.MessageCount("data"));
                DebugLog("Success!");
            }
            catch
            {
                LogEventHappened?.Invoke("Error occured during consuming messages", LogLevel.Error);
                throw;
            }
            
        }

        private void DebugLog(string message)
        {
            LogEventHappened?.Invoke(message, LogLevel.Debug);
        }

        private static void CreateQueue(IModel channel, string name, bool durable = false, bool autoDelete = false)
        {
            channel.QueueDeclare(queue: name,
                durable: durable,
                exclusive: false,
                autoDelete: autoDelete,
                arguments: null);
        }
    }
}
