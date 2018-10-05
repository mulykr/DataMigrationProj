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

        public event MakeLog LogEventHappened;

        public RabbitMqClient(string host, string vhost)
        {
            _connectionFactory = new ConnectionFactory
            {
                Uri = new Uri(host),
                VirtualHost = vhost
            };
        }

        private void DebugLog(string message)
        {
            LogEventHappened?.Invoke(message, LogLevel.Debug);
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
            DebugLog("Publishing message to queue...");
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
            DebugLog("Message was sent. Success!");
        }

        public void ConsumeMessageFromQueue(string queueName, EventHandler<BasicDeliverEventArgs> handler)
        {
            DebugLog("Consuming message from queue...");
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
            DebugLog("Success!");
        }
    }
}
