from kafka import KafkaProducer
import pandas as pd
import json
import time
import logging

# Function to send messages to Kafka topic
def send_message(logger, producer):
    try: 
        data = pd.read_csv('C:/airqual-project/air-quality-Kafka-pipeline/consumed_air_quality_data.csv')
        testing_data = data.iloc[int(len(data)*0.8):]
        for i in range(len(testing_data)):
            message = testing_data.iloc[i].to_dict()
            producer.send('air-qual-test', message)
            logger.info('Message sent successfully.')
            # time.sleep(1) # Time delay mechanism simulating hourly readings
    except Exception as e:
        logger.error(f'Failed to send message: {e}')

if __name__ == '__main__':
    # Set up logging
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename= 'airqual-producer-log.txt', filemode='w')
    
    # Kafka Producer setup
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                                value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        logger.info('Kafka Producer set up successfully.')
    except Exception as e:
        logger.error('Failed to set up Kafka Producer: {e}')
        raise
    
    try: 
        send_message(logger, producer)
    finally: 
        producer.flush()
        producer.close()