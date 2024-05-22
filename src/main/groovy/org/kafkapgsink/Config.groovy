package org.kafkapgsink

class Config {
	Kafka kafka
	Topic topic
	Quarantine quarantine
	DeadLetter deadletter
	Postgres postgres

	class Topic {
		String name
	}


	class Quarantine {
		String topicName
		int maxRetries = 3
	}

	class DeadLetter {
		String topicName
	}

	class Postgres {
		String host
		String dbName
		String user
		String password
		String table
		List<Field> schema
	}

	class Kafka {
		List<String> bootstrapServers
		String consumerGroupId
		String autoOffsetReset
	}
}

class Field {
	String name
	String type
}
