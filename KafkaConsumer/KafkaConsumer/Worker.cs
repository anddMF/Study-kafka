using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsumer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Iniciando consumo kafka");

            string[] args = Environment.GetCommandLineArgs();
            args = args.Skip(1).ToArray();

            if(args.Length != 2)
            {
                _logger.LogError("Informe 2 parametros: \n1 - IP/porta para testes com o Kafka \n2 - Topic que vai ser utilizado");
                return;
            }

            string bootstraperServers = args[0];
            string nameTopic = args[1];

            _logger.LogInformation($"BootstraperServers = {bootstraperServers}");
            _logger.LogInformation($"Topic = {nameTopic}");

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstraperServers,
                GroupId = $"{nameTopic}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using(var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(nameTopic);
                    try
                    {
                        while (true)
                        {
                            var cr = consumer.Consume(cts.Token);
                            _logger.LogInformation($"Mensagem lida: {cr.Message.Value}");
                        }
                    } catch(Exception ex)
                    {
                        consumer.Close();
                        _logger.LogWarning("Cancelada a execucao do consumer...");
                    }
                }
            } catch (Exception ex)
            {
                _logger.LogError($"Error: {ex.Message} | {ex.GetType().FullName}");
            }
        }
    }
}
