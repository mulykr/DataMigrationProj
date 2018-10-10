using System;
using System.Text;
using DataMigration.Logger;
using DataMigration.Logger.Enums;
using RabbitMQ.Client;

namespace DataMigration.Models
{
    public class RabbitMqProducer : ILogSupporting, IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private IConnection _connection;
        private IModel _publishingChannel;
        private IConnection Connection => _connection ?? (_connection = _connectionFactory.CreateConnection());
        private IModel PublishingChannel => _publishingChannel ?? (_publishingChannel = Connection.CreateModel());

        public RabbitMqProducer(string host, string vhost)
        {
            _connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(host),
                VirtualHost = vhost
            };
        }

        public event MakeLog LogEventHappened;

        public bool PublishMessageToQueue(string queueName, string message, string routingKey)
        {
            try
            {
                CreateQueue(PublishingChannel, queueName);

                var body = Encoding.UTF8.GetBytes(message);

                PublishingChannel.BasicPublish(exchange: string.Empty,
                    routingKey: routingKey,
                    basicProperties: null,
                    body: body);
                return true;
            }
            catch (Exception exception)
            {
                LogEventHappened?.Invoke($"Error occured while sending message: {exception.Message}", LogLevel.Error);
                return false;
            }
        }

        public void Dispose()
        {
            _connection?.Dispose();
            _publishingChannel?.Dispose();
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
