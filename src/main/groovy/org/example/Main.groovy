package org.example

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.databind.ObjectMapper
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Duration
import java.util.concurrent.Future
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory


static void main(String[] args) {
	def log = LoggerFactory.getLogger(Main)
	def config
	try {
		config = new Config()
	} catch (IOException e) {
		log.error("Could not load config.properties", e)
	}

	def pg = new PostgresClient()
	def handler = new KafkaHandler(new Consumer(), new QuarantineProducer())
	handler.start(config.topic.name, { ConsumerRecord record -> pg.insertPayment(record.value() as String) })
}

class Consumer {
	KafkaConsumer consumer
	def bootstrapServers = "127.0.0.1:9092"
	def groupId = "my-fourth-application"

	Consumer() {
		def props = new Properties()
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.getName())
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.getName())
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
		consumer = new KafkaConsumer<>(props)
	}

	void subscribe(List<String> topics) {
		consumer.subscribe(topics)
	}

	ConsumerRecords poll(Duration dur) {
		return consumer.poll(dur)
	}
}

class QuarantineProducer {
	KafkaProducer producer
	def bootstrapServers = "127.0.0.1:9092"
	def groupId = "my-fourth-application"

	QuarantineProducer() {
		def props = new Properties()
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
		producer = new KafkaProducer<>(props)
	}

	Future<RecordMetadata> send(String key, String value) {
		ProducerRecord quarantineRecord = new ProducerRecord("demo_java_quarantine", key, value)
		producer.send(quarantineRecord, )
		producer.flush()
	}
}

class KafkaHandler {
	Consumer consumer
	QuarantineProducer quarantineProducer
	def logger = LoggerFactory.getLogger(KafkaHandler)

	KafkaHandler(Consumer consumer, QuarantineProducer quarantineProducer) {
		this.consumer = consumer
		this.quarantineProducer = quarantineProducer
	}

	void start(String topic, Closure callback) {
		consumer.subscribe(List.of(topic))

		//noinspection GroovyInfiniteLoopStatement
		while (true) {
			def records = consumer.poll(Duration.ofMillis(1000))
			records.each {
				try {
					callback(it)
				} catch (Exception e) {
					quarantineProducer.send(it.key(), it.value())
				}
			}
		}
	}
}

class Payment {
	String id
	Timestamp time
	float amount
}

class PostgresClient {
	Connection connection
	def url = "jdbc:postgresql://localhost/djangopg"
	def objectMapper = new ObjectMapper()
	def logger = LoggerFactory.getLogger(PostgresClient)

	PostgresClient() {
		Properties props = new Properties()
		props.setProperty("user", "postgres")
		props.setProperty("password", "mysecretpassword")
		connection = DriverManager.getConnection(url, props)
	}

	void insertPayment(String data) {
		def row
		try {
			row = objectMapper.readValue(data, Payment)
		} catch (Exception e) {
			logger.error("could not deserialize data", e)
		}

		logger.debug("$row.amount, $row.time, $row.id")
		try {
			connection.createStatement().execute("insert into main_payment(id, amount, time) values ('$row.id', $row.amount, timestamp '$row.time')")
		} catch (Exception e) {
			logger.error("couldn't insert row", e)
		}
	}
}
