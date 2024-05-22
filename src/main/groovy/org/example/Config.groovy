package org.example

class Config {
	Topic topic
	Quarantine quarantine
	DeadLetter deadletter
	Postgres postgres

	class Topic {
		String name
		List<Field> schema
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
	}
}

class Field {
	String name
	String type
}
