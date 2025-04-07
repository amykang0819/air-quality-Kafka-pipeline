from kafka import KafkaConsumer
import json
import time
from datetime import datetime
import logging
import pandas as pd
import joblib

# Function to consume messages from the Kafka topic
def consume_message(logger, consumer, model, feature_cols, timeout=60):
    start_time = time.time()
    received_messages = []
    predictions = []
    print('Starting consumer...')
    try: 
        while time.time() - start_time <= timeout:
            records = consumer.poll(timeout_ms=1000)
            for topic_partition, messages in records.items():
                for message in messages:
                    print(f"Received: {message.value}")
                    logger.info(f"Received: {message.value}")
                    received_messages.append(message.value)
                    
                    # DataFrame creation & preprocessing
                    data = pd.DataFrame([message.value])
                    data['Datetime'] = pd.to_datetime(data['Datetime'])
                    data.set_index('Datetime', inplace=True)
                    
                    # Time-based features
                    data['Hour'] = data.index.hour
                    data['Day'] = data.index.day
                    data['Month'] = data.index.month
                    data['Year'] = data.index.year
                    data = data.reindex(columns=feature_cols, fill_value=0)
                    
                    # Prediction
                    prediction = model.predict(data)
                    print(f'Predicted Air Quality: {prediction}')
                    logger.info(f'Predicted Air Quality: {prediction}')

                    # Append to predictions list
                    result = message.value.copy()
                    result['Predicted Air Quality'] = prediction
                    predictions.append(result)

                    # Save to CSV
                    df = pd.DataFrame(predictions)
                    df.to_csv('C:/airqual-project/air-quality-Kafka-pipeline/phase_3_model_prediction/air_quality_predictions.csv', index=False)
                    print('Saved predictions to CSV file.')
                    logger.info('Saved predictions to CSV file.')

    except Exception as e:
        logger.error(f'Failed to consume message: {e}')
    finally:
        consumer.close()
        # df = pd.DataFrame(received_messages) 
        # df.to_csv('consumed_air_quality_data.csv', index=False)
        # print('Saved consumed data to CSV file.') 
        # logger.info('Saved consumed data to CSV file.') 

if __name__ == '__main__':
    # Set up logging
    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        filename= 'airqual-consumer-log.txt', filemode='w')
    
    # Load the pre-trained model 
    try:
        model = joblib.load('C:/airqual-project/air-quality-Kafka-pipeline/phase_3_model_prediction/air_quality_model.pkl')
        feature_cols = joblib.load('C:/airqual-project/air-quality-Kafka-pipeline/phase_3_model_prediction/model_features.pkl')
        logger.info('Model and feature columns loaded successfully.')
    except Exception as e:
        logger.error(f'Failed to load model or features: {e}')
        raise

    # Kafka Consumer setup
    try: 
        consumer = KafkaConsumer(
            'air-qual-test',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',  
            enable_auto_commit=True,  
            value_deserializer=lambda v: json.loads(v.decode('utf-8')))
        # time.sleep(1) # Time delay mechanism simulating hourly data consumption
        logger.info('Kafka Consumer set up successfully.')
        consume_message(logger, consumer, model, feature_cols, timeout=60)
    except Exception as e: 
        logger.error(f'Failed to set up Kafka Consumer: {e}')