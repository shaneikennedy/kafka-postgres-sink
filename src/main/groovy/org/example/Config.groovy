package org.example

class Config {
	Topic topic
	Quarantine quarantine
	DeadLetter deadletter

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
}
