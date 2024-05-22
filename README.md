# Kafka to postgres sink
An open source version of the confluent postgres sink, written in groovy
## Features
- Retry-ability with quarantining for maintaining high throughput
- Dead letter queues for failed messages
- No need to setup Kafka Connect, drop this application wherever it has access to your kafka cluster and postgres


## Todos
- Add a jitter/exponential backoff to retried jobs
- Integrate with schema registry to be able to serialize based on how the data is represented on Kafka
- All the security stuff, please don't use this yet
- Docker file and image pushed to docker hub to make this easy to deploy

### Example configuration
```yaml
kafka:
  bootstrapServers:
    - 127.0.0.1:9092
  consumerGroupId: "why-groovy"
  autoOffsetReset: "latest"

# Main topic to sink to pg
topic:
  name: demo_java

# Quarantine strategy
quarantine:
  enabled: true
  topicName: demo_java_quarantine
  maxRetries: 3

# Dead letter queuing
deadletter:
  enabled: true
  topicName: demo_java_dlq


postgres:
  host: localhost
  dbName: djangopg
  user: postgres
  password: mysecretpassword
  table: main_payment
  schema:
    - name: id
      type: string
    - name: time
      type: timestamp
    - name: amount
      type: number
```