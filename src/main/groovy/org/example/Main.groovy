package org.example

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

static void main(String[] args) {
	def log = LoggerFactory.getLogger(Main)
	def config
	try {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory())
		def configFile = this.getClass().getClassLoader()
				.getResourceAsStream("config.yaml")
		config = mapper.readValue(configFile, Config)
	} catch (IOException e) {
		log.error("Could not load config.yaml", e)
	}

	log.info("max retries configured to $config.quarantine.maxRetries")
	def pg = new PostgresClient(config.postgres.host, config.postgres.dbName, config.postgres.user, config.postgres.password)

	def handler = new KafkaHandler(
			new Consumer(),
			new QuarantineProducer(config.quarantine.topicName),
			new DeadLetterProducer(config.deadletter.topicName))
	handler.start(
			List.of(config.topic.name, config.quarantine.topicName), { ConsumerRecord record ->
				pg.insertPayment(record.value() as String)
			})
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

class DeadLetterProducer {
	KafkaProducer producer
	String topic
	def bootstrapServers = "127.0.0.1:9092"
	def groupId = "my-fourth-application"

	DeadLetterProducer(String topicName) {
		this.topic = topicName
		def props = new Properties()
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
		producer = new KafkaProducer<>(props)
	}

	void sendSynchronously(String key, String value, Headers headers) {
		// TODO do better with the partition number, the constructor here is annoying
		ProducerRecord quarantineRecord = new ProducerRecord(topic, 0, key, value, headers)
		producer.send(quarantineRecord)
		producer.flush()
	}
}

class QuarantineProducer {
	KafkaProducer producer
	String topic
	def bootstrapServers = "127.0.0.1:9092"
	def groupId = "my-fourth-application"

	QuarantineProducer(String topicName) {
		this.topic = topicName
		def props = new Properties()
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
		producer = new KafkaProducer<>(props)
	}

	void sendSynchronously(String key, String value, Headers headers) {
		// "increment" the retry count
		headers.add("retry-attempt", 0.byteValue())

		// TODO do better with the partition number, the constructor here is annoying
		ProducerRecord quarantineRecord = new ProducerRecord(topic, 0, key, value, headers)
		producer.send(quarantineRecord)
		producer.flush()
	}
}

class KafkaHandler {
	Consumer consumer
	QuarantineProducer quarantineProducer
	DeadLetterProducer dlqProducer
	def logger = LoggerFactory.getLogger(KafkaHandler)

	KafkaHandler(Consumer consumer, QuarantineProducer quarantineProducer, DeadLetterProducer dlqProducer) {
		this.consumer = consumer
		this.quarantineProducer = quarantineProducer
		this.dlqProducer = dlqProducer
	}

	void start(List<String> topics, Closure callback) {
		consumer.subscribe(topics)

		//noinspection GroovyInfiniteLoopStatement
		while (true) {
			def records = consumer.poll(Duration.ofMillis(1000))
			records.each {
				try {
					callback(it)
				} catch (Exception e) {
					def retryCount = it.headers().count {it.key().equals("retry-attempt")}
					logger.info("RETRY COUNT: $retryCount")
					if (retryCount > 3) {
						logger.error("Dropping message after 3 retries")
						dlqProducer.sendSynchronously(it.key(), it.value(), it.headers())
					} else {
						quarantineProducer.sendSynchronously(it.key(), it.value(), it.headers())
					}
				}
			}
		}
	}
}

class Payment {
	String id
	@JsonFormat (pattern = "yy-mm-dd HH:mm:ss")
	Timestamp time
	float amount
}

class PostgresClient {
	Connection connection
	def objectMapper = new ObjectMapper()
	def logger = LoggerFactory.getLogger(PostgresClient)


	PostgresClient(String host, String dbName, String user, String password) {
		Properties props = new Properties()
		props.setProperty("user", user)
		props.setProperty("password", password)
		connection = DriverManager.getConnection(constructURL(host, dbName), props)
	}

	String constructURL(String host, String dbName) {
		return "jdbc:postgresql://$host/$dbName"
	}

	void insertPayment(String data) {
		def row
		try {
			row = objectMapper.readValue(data, Payment)
		} catch (Exception e) {
			logger.debug("could not deserialize data", e)
		}

		logger.debug("$row.amount, $row.time, $row.id")
		try {
			connection.createStatement().execute("insert into main_payment(id, amount, time) values ('$row.id', $row.amount, timestamp '$row.time')")
		} catch (Exception e) {
			logger.debug("couldn't insert row", e)
		}
	}
}
