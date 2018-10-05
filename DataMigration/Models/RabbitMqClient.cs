using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DataMigration.Models
{
    using Logger;
    using Logger.Enums;
    class RabbitMqClient : ILogSupporting, IDisposable
    {
        private readonly string _host;
        private readonly string _vhost;
        private ConnectionFactory _connectionFactory;
        public event MakeLog LogEventHappened;

        public RabbitMqClient(string host, string vhost)
        {
            _host = host;
            _vhost = vhost;
            _connectionFactory = new ConnectionFactory();
            _connectionFactory.Uri = new Uri(_host);
            _connectionFactory.VirtualHost = _vhost;
        }

        private void DebugLog(string message)
        {
            LogEventHappened?.Invoke(message, LogLevel.Debug);
        }
        private void ErrorLog(string message)
        {
            LogEventHappened?.Invoke(message, LogLevel.Error);
        }
        
        public void CreateQueue(IModel channel, string name, bool durable = false, bool autoDelete = false)
        {
            channel.QueueDeclare(queue: name,
                                 durable: durable,
                                 exclusive: false,
                                 autoDelete: autoDelete,
                                 arguments: null);
        }

        public void PublishMessageToQueue(string queueName, string message, string routingKey)
        {
            using (var connection = _connectionFactory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    CreateQueue(channel, queueName);

                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                                         routingKey: routingKey,
                                         basicProperties: null,
                                         body: body);
                }
            }
        }

        public void ConsumeMessageFromQueue(string queueName, EventHandler<BasicDeliverEventArgs> handler)
        {
            using (var connection = _connectionFactory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    CreateQueue(channel, queueName);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += handler;
                    channel.BasicConsume(queue: queueName,
                                         autoAck: true,
                                         consumer: consumer);
                    System.Threading.Thread.Sleep(1000);
                }
            }
        }

        public void Dispose()
        {
        }
    }
}
