package org.example

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import java.sql.Connection
import java.sql.DriverManager
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
	def pg = new PostgresClient(config.postgres)

	def handler = new KafkaHandler(
			new Consumer(config.kafka),
			new QuarantineProducer(config.kafka, config.quarantine),
			new DeadLetterProducer(config.kafka, config.deadletter))
	handler.start(
			List.of(config.topic.name, config.quarantine.topicName), { ConsumerRecord record ->
				pg.insertRecord(record.value() as String)
			})
}

class Consumer {
	KafkaConsumer consumer

	Consumer(Config.Kafka kafka) {
		def props = new Properties()
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers.join(","))
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.getName())
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.getName())
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, kafka.consumerGroupId)
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafka.autoOffsetReset)
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

	DeadLetterProducer(Config.Kafka kafka, Config.DeadLetter config) {
		this.topic = config.topicName
		def props = new Properties()
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers.join(","))
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		producer = new KafkaProducer<>(props)
	}

	void sendSynchronously(String key, String value, Headers headers) {
		// partition set to null because the kafka client handles it during producer.send()
		ProducerRecord quarantineRecord = new ProducerRecord(topic, null, key, value, headers)
		producer.send(quarantineRecord)
		producer.flush()
	}
}

class QuarantineProducer {
	KafkaProducer producer
	String topic

	QuarantineProducer(Config.Kafka kafka, Config.Quarantine config) {
		this.topic = config.topicName
		def props = new Properties()
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers.join(","))
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.getName())
		producer = new KafkaProducer<>(props)
	}

	void sendSynchronously(String key, String value, Headers headers) {
		// "increment" the retry count
		headers.add("retry-attempt", 0.byteValue())

		// partition set to null because the kafka client handles it during producer.send()
		ProducerRecord quarantineRecord = new ProducerRecord(topic, null, key, value, headers)
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
					logger.info("Successfully wrote to pg for key: $it.key")
				} catch (Exception e) {
					logger.error("Exception: {}", e.toString())
					def retryCount = it.headers().count {it.key().equals("retry-attempt")}
					logger.info("RETRY COUNT: $retryCount")
					if (retryCount > 3) {
						logger.error("Dropping message after 3 retries")
						dlqProducer.sendSynchronously(it.key(), it.value(), it.headers())
					} else {
						logger.info("Sending to quarantine")
						quarantineProducer.sendSynchronously(it.key(), it.value(), it.headers())
					}
				}
			}
		}
	}
}

class PostgresClient {
	Connection connection
	String dbName
	List<Field> schema
	String table
	def objectMapper = new ObjectMapper()
	def logger = LoggerFactory.getLogger(PostgresClient)


	PostgresClient(Config.Postgres config) {
		Properties props = new Properties()
		props.setProperty("user", config.user)
		props.setProperty("password", config.password)
		this.dbName = config.dbName
		this.schema = config.schema
		this.table = config.table
		connection = DriverManager.getConnection(constructURL(config.host, config.dbName), props)
	}

	static String constructURL(String host, String dbName) {
		return "jdbc:postgresql://$host/$dbName"
	}

	String constructSQLInsert(String data) {
		// Setup the insert statement: insert into <tableName> (field1, field2, field3...
		def fieldName = schema.collect {it.name}
		def insertStatement = "insert into $table ("
		fieldName.each {
			insertStatement += "$it, "
		}

		// Replace the last two characters, a comma and a space, with a close-parens
		insertStatement = insertStatement.substring(0, insertStatement.length() - 2)

		// Insert the values
		insertStatement += ") values ("

		// Deserialize the data to a map that we can lookup values for based on schema
		Map row
		try {
			row = objectMapper.readValue(data, Map)
		} catch (Exception e) {
			logger.debug("could not deserialize data", e)
		}

		// Insert each value from schema
		schema.each {
			def value = row.get(it.name)
			if (it.type.equals("string")) {
				insertStatement += "'$value', "
			} else if (it.type.equals("timestamp")) {
				insertStatement += "timestamp '$value', "
			} else {
				insertStatement += "$value, "
			}
		}

		// Replace the last two characters, a comma and a space, with a close-parens
		insertStatement = insertStatement.substring(0, insertStatement.length() - 2)

		// Close the statement
		insertStatement += ")"

		logger.debug("Generated insert statement: $insertStatement")

		return insertStatement
	}

	void insertRecord(String data) throws Exception {
		connection.createStatement().execute(constructSQLInsert(data))
	}
}
