using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaProducer
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
            _logger.LogInformation("Teste servico de envio de mensagens");
            string[] args = Environment.GetCommandLineArgs();
            args = args.Skip(1).ToArray();

            if (args.Length < 3)
            {
                _logger.LogError("Informe ao menos 3 parametros: \n1 - IP/porta para testes com o Kafka, \n2 - o Topic que recebera a mensagem; \n3 - a partir do terceiro sao as mensagens");
                return;
            }

            string bootstrapServer = args[0];
            string nameTopic = args[1];

            _logger.LogInformation($"BootstrapServer = {bootstrapServer}");
            _logger.LogInformation($"Topic = {nameTopic}");
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServer
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 2; i < args.Length; i++)
                    {
                        string message = args[i];
                        var result = await producer.ProduceAsync(
                            nameTopic,
                            new Message<Null, string> { Value = message });

                        _logger.LogInformation($"Message: {message} | Status: {result.Status.ToString()}");
                    }

                    _logger.LogInformation("Concluido envio de mensages");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Erro: {ex.Message} | {ex.GetType().FullName}");
            } 
        }
    }
}
