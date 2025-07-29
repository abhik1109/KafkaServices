using Confluent.Kafka;
using System.Text.Json;

namespace KafkaProducerBase
{
	public class KafkaPublisher
	{
		private readonly ProducerConfig _producerConfig;

		public KafkaPublisher(string kafkaBootstrapServerUrl, string clientId, string brokerAddressFamily = "1")
		{
			var isValidBrokerAddressFamily = Enum.TryParse(typeof(BrokerAddressFamily), brokerAddressFamily, out var kafkaBrokerAddFamily);
			if (!isValidBrokerAddressFamily)
			{
				throw new Exception("Invalid BrokerAddressFamily value.");
			}

			_producerConfig = new ProducerConfig
			{
				BootstrapServers = kafkaBootstrapServerUrl,
				ClientId = clientId,
				BrokerAddressFamily = (BrokerAddressFamily)kafkaBrokerAddFamily
			};

			
		}
		public async Task<DeliveryResult<Null, string>> PublishMessage(string topicName, object publishData)
		{
			using var producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
			var jsonData=JsonSerializer.Serialize(publishData);
			var message = new Message<Null, string>
			{
				Value = jsonData
			};
			
			var deliveryReport = await producer.ProduceAsync(topicName, message);

			return deliveryReport;
		}
	}
}
