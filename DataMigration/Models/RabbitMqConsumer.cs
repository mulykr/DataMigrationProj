using System;
using DataMigration.Logger;
using DataMigration.Logger.Enums;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DataMigration.Models
{
    public class RabbitMqConsumer : ILogSupporting, IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private IConnection _connection;
        private IModel _consumingChannel;
        private IConnection Connection => _connection ?? (_connection = _connectionFactory.CreateConnection());
        private IModel ConsumingChannel => _consumingChannel ?? (_consumingChannel = Connection.CreateModel());

        public RabbitMqConsumer(string host, string vhost)
        {
            _connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(host),
                VirtualHost = vhost
            };
        }

        public event MakeLog LogEventHappened;
        
        public void ConsumeMessagesFromQueue(string queueName, EventHandler<BasicDeliverEventArgs> handler)
        {
            try
            {
                DebugLog("Consuming message from queue...");
                CreateQueue(ConsumingChannel, queueName);

                var consumer = new EventingBasicConsumer(ConsumingChannel);
                consumer.Received += handler;

                var messageCount = ConsumingChannel.MessageCount(queueName);
                DebugLog($"Pending to be consumed: {messageCount}");
                ConsumingChannel.BasicConsume(queue: queueName,
                    autoAck: true,
                    consumer: consumer);
            }
            catch
            {
                LogEventHappened?.Invoke("Error occured during consuming messages", LogLevel.Error);
                throw;
            }

        }

        public void Dispose()
        {
            _consumingChannel?.Dispose();
            _connection?.Dispose();
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
