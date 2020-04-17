package mdhu.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
		
		String bootstrapServers = "127.0.0.1:9092";
		
		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// run the function several times, and you should notice that the same key always goes to the same partition
		for (int i = 0; i < 10; ++i) {
			// create a producer record
			
			String topic = "first_topic";
			String value = "hello_world " + Integer.toString(i);
			String key = "id_" + Integer.toString(i);
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			
			logger.info("Key: " + key);
			
			// send data
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					// executes every time a record is successfully sent or an exception is thrown
					if (e == null) {
						// the record was successfully sent
						logger.info("Received new metadata. \n" +
						"Topic: " + recordMetadata.topic() + "\n" + 
						"Partition: " + recordMetadata.partition() + "\n" +
						"Offset: " + recordMetadata.offset() + "\n" + 
						"Timestamp: " + recordMetadata.timestamp());
					}
					else {
						logger.error("Error while producing", e);
					}
				}
			}).get(); // get() blocks the send() to be synchronous -- which is a bad practice!! but here just for demo purpose
		}
		
		// flush data
		producer.flush();
		// flush data and close producer
		producer.close();
	}
}
