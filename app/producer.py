import requests
from kafka import KafkaProducer
import time,json


def get_data(page) :
    data = requests.get(f"http://127.0.0.1:5000/api/movies/{page}")
    return data.json()


def serializer(message):
    return json.dumps(message).encode('utf-8')

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
     value_serializer= serializer
    )


# Specify the topic to which you want to produce messages
topic = 'MovieRecommender'

if __name__ == '__main__':

    while True:
        
        for page in range(0, 9999):

            movies = get_data(page)

            for movie in movies :
                
                producer.send(topic, value=movie)
                producer.flush()
                print(f"Produced data for page {page} Message {movie}")
                time.sleep(3)

