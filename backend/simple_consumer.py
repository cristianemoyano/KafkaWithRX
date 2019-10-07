from kafka import KafkaConsumer
import json

server = 'kafka:9093'
topic_name = "my-topic"


print("Set consumer")
consumer = KafkaConsumer(topic_name,
                         auto_offset_reset='earliest',
                         bootstrap_servers=[server],
                         api_version=(0, 10),
                         value_deserializer=json.loads,
                         consumer_timeout_ms=1000)

print("Read")
# for msg in consumer:
#     print(msg.key.decode("utf-8"), msg.value)


while True:
    raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
    for topic_partition, messages in raw_messages.items():
        for message in messages:
            print("{msg} - {t}".format(msg=message.value, t=message.timestamp))
