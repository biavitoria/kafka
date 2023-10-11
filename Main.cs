/*
  Nome: Beatriz Vitoria Brandao Silva - 756202
  Professor: Wagner Cipriano da Silva
  Disciplina: Desenvolvimento de Aplicações Distribuídas
  Descricao: Implementar um producer, que irá enviar a mensagem para um tópico específico no Event Hub, utilizando C#
*/

using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        string brokerList = "puc-dad.servicebus.windows.net:9093";
        string topicName = "dad-atividade-kafka";
        string principal = "$ConnectionString";
        string secret = "Endpoint=sb://puc-dad.servicebus.windows.net/;SharedAccessKeyName=aluno-dad;SharedAccessKey=Kds6a1hYMueVSSbu7bgiKXBpjBbzT4Kol+AEhNOt3FQ=";

        var config = new ProducerConfig
        {
            BootstrapServers = brokerList,
            SaslMechanism = SaslMechanism.Plain,
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslUsername = principal,
            SaslPassword = secret,

        };

         var producer = new ProducerBuilder<string, string>(config).Build();

        try
        {
            var data = new
            {
                name = "Beatriz Vitoria Brandao Silva",
                login_id = "1365605@sga.pucminas.br",
                group = 5
            };

            string jsonValue = JsonConvert.SerializeObject(data);

            var message = new Message<string, string>
            {
                Key = null,
                Value = jsonValue
            };

            var deliveryReport = await producer.ProduceAsync(topicName, message);
            Console.WriteLine("Mensagem enviada: " + message.Value.ToString());                                                         
            Console.ReadKey();
            Console.WriteLine($"Mensagem enviada para: {deliveryReport.TopicPartitionOffset}");
        }
        catch (ProduceException<Null, string> e)
        {
            Console.WriteLine($"Erro ao enviar mensagem: {e.Error.Reason}");
        }
    }
}
