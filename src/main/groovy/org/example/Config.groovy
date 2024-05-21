package org.example

class Config {
	Topic topic
	Quarantine quarantine
	DeadLetter dlq

	Config() throws IOException {
		def props = new Properties()
		props.load(Main.getClassLoader().getResourceAsStream("config.properties"))
		topic = new Topic(name: props.getProperty("topicName"))
	}

	class Topic {
		String name
	}

	class Quarantine {
		String name
	}

	class DeadLetter {
		String name
	}
}
