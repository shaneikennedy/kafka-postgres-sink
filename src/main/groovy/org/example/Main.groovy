package org.example

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.databind.ObjectMapper
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory


static void main(String[] args) {
	def pg = new PostgresClient()
	def consumer = new Consumer()
	consumer.start("demo_java", { ConsumerRecord record -> pg.insertPayment(record.value() as String)})
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

	void start(String topic, Closure callback) {
		consumer.subscribe(List.of(topic))

		//noinspection GroovyInfiniteLoopStatement
		while (true) {
			def records = consumer.poll(Duration.ofMillis(1000))
			records.each {
				callback(it)
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