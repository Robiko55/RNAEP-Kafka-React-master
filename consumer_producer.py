from kafka import KafkaConsumer, KafkaProducer
import json

# Inicijalizacija Kafka consumer za prvi topic
consumer_topic1 = KafkaConsumer('topic1', bootstrap_servers='localhost:9092', group_id='consumer_producer_group')

# Inicijalizacija Kafka producer za drugi topic
producer_topic2 = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')

# Pocetak procesiranja porukaa
print("Obrada poruka...")

# Čitanje, obrada i slanje podataka
for message in consumer_topic1:
    # Decode and load JSON data from the Kafka message
    weather_data = json.loads(message.value.decode('utf-8'))

    # dodavanje ključa "processed" sa vrednošću True
    processed_data = {"processed": True, **weather_data}
    
    # Slanje obradjenih podataka na drugi topic
    producer_topic2.send('topic2', value=json.dumps(processed_data).encode('utf-8'))

# Kraj procesiranja poruka
print("Consumer-producer finished processing messages.")

# Zatvaranje consumer-a za prvi topic i producera za drugi topic
consumer_topic1.close()
producer_topic2.close()
