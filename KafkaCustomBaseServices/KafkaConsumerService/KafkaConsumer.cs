using Confluent.Kafka;
using static Confluent.Kafka.ConfigPropertyNames;

namespace KafkaConsumerBase
{
	public class KafkaConsumer
	{
		private readonly ConsumerConfig _consumerConfig;
		private readonly IConsumer<Ignore, string> _consumer;

		/// <summary>
		/// kafka consumer service parameterized constructor
		/// </summary>
		/// <param name="kafkaBootstrapSrverUrl">Bootstrap url for kafka broker service hosted</param>
		/// <param name="clientId">Client ID for kafka consumer configuration</param>
		/// <param name="groupId">Group ID for kafka consumer configuraton</param>
		/// <param name="brokerAddressFamily">Kafka broker url family e.g. V4/V6</param>
		/// <exception cref="Exception"></exception>
		public KafkaConsumer(string kafkaBootstrapSrverUrl, string clientId, string groupId, string topicName, string brokerAddressFamily = "1")
		{
			var isValidBrokerAddressFamily = Enum.TryParse(typeof(BrokerAddressFamily), brokerAddressFamily, out var kafkaBrokerAddFamily);
			if (!isValidBrokerAddressFamily)
			{
				throw new Exception("Invalid BrokerAddressFamily value.");
			}

			var consumerConfig = new ConsumerConfig
			{
				BootstrapServers = kafkaBootstrapSrverUrl,
				AutoOffsetReset = AutoOffsetReset.Earliest,
				ClientId = clientId,
				GroupId = groupId,
				BrokerAddressFamily = (BrokerAddressFamily)kafkaBrokerAddFamily
			};
			_consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
			_consumer.Subscribe(topicName);
		}
		/// <summary>
		/// Consume kafka message based on specific topic and message key (optional)
		/// </summary>
		/// <param name="topicName">Kafka topic name under message published</param>
		/// <param name="messageKey">Specific message key to fetch specific message</param>
		/// <returns>List of string</returns>
		public async Task<List<string>> ConsumeMessage(string topicName, string? messageKey = null)
		{
			
			List<ConsumeResult<Ignore, string>> lstMessage = new();
			try
			{
				while (true)
				{
					var consumeResult = _consumer.Consume();
					lstMessage.Add(consumeResult);
				}
				return messageKey==null? lstMessage.Select(x => x.Message.Value).ToList()
					:lstMessage.Where(x=>x.Message.Key.ToString()==messageKey).Select(x=>x.Message.Value).ToList();
			}
			catch (Exception ex)
			{
				throw;
			}
			finally
			{
				_consumer.Close();
				
			}
			

		}
	}
}
