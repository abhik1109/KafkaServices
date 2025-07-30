using Confluent.Kafka;
using System.Text.Json;

namespace KafkaProducerBase
{
	public class KafkaPublisher
	{
		private readonly ProducerConfig _producerConfig;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="kafkaBootstrapServerUrl"></param>
		/// <param name="clientId"></param>
		/// <param name="brokerAddressFamily"></param>
		/// <exception cref="Exception"></exception>
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

		/// <summary>
		/// 
		/// </summary>
		/// <param name="topicName"></param>
		/// <param name="publishData"></param>
		/// <returns></returns>
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
