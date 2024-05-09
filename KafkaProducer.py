import random
import string
import time
import threading
from kafka import KafkaProducer

# Kafka configuration
kafka_config = {
    'bootstrap_servers': 'localhost:9092',  # Kafka broker address and port
    'acks': 0,
    'retries': 1,
    'batch_size': 16384,  # 16KB batch size
    'buffer_memory': 33554432,  # 32MB buffer
    'key_serializer': str.encode,  # Serializer for message key (string)
    'value_serializer': str.encode,  # Serializer for message value (string)
    'linger_ms': 1
}

# Create Kafka producer
producer = KafkaProducer(**kafka_config)

producer.send('viewrecords', value="word", key="word")

def generate_random_word(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


def produce_messages():
    while True:
        # Generate 50 random words per second
        for _ in range(10):
            word = generate_random_word(random.randint(5, 10))
            producer.send('viewrecords', value=word, key=word)
            print(word)
        time.sleep(1)  # Sleep for 1 second


# Start producing messages in a separate thread
producer_thread = threading.Thread(target=produce_messages)
producer_thread.daemon = True
producer_thread.start()

# Keep the main thread running
while True:
    time.sleep(1)