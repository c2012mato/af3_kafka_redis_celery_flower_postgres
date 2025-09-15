"""
Sample DAG demonstrating streaming pipeline with Kafka, Redis, and PostgreSQL
Updated for Airflow 3.0.0 compatibility
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator  # Updated import
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis_hook import RedisHook
import json
import logging
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import random

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG with Airflow 3.0.0 optimized settings
dag = DAG(
    'streaming_pipeline_demo',
    default_args=default_args,
    description='A sample streaming pipeline DAG - Airflow 3.0.0 compatible',
    schedule=timedelta(minutes=30),  # Updated from schedule_interval
    catchup=False,
    tags=['streaming', 'kafka', 'redis', 'postgres', 'airflow-3.0'],
    max_active_runs=1,  # Optimized for 3.0.0
)

def create_sample_data(**context):
    """Generate sample streaming data"""
    data = []
    for i in range(100):
        record = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'user_id': random.randint(1, 1000),
            'event_type': random.choice(['click', 'view', 'purchase', 'signup']),
            'value': random.uniform(10.0, 1000.0),
            'metadata': {
                'source': 'web',
                'device': random.choice(['mobile', 'desktop', 'tablet'])
            }
        }
        data.append(record)
    
    # Store in XCom for next task
    return json.dumps(data)

def produce_to_kafka(**context):
    """Send data to Kafka topic"""
    try:
        # Get data from previous task
        data_json = context['task_instance'].xcom_pull(task_ids='generate_sample_data')
        data = json.loads(data_json)
        
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Send messages to Kafka
        topic = 'streaming_events'
        for record in data:
            producer.send(topic, record)
            logging.info(f"Sent record to Kafka: {record['id']}")
        
        producer.flush()
        producer.close()
        
        logging.info(f"Successfully sent {len(data)} records to Kafka topic: {topic}")
        return f"Sent {len(data)} records to Kafka"
        
    except Exception as e:
        logging.error(f"Error producing to Kafka: {str(e)}")
        raise

def consume_from_kafka_to_redis(**context):
    """Consume from Kafka and cache in Redis"""
    try:
        # Initialize Kafka consumer
        consumer = KafkaConsumer(
            'streaming_events',
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000,  # 10 seconds timeout
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        # Initialize Redis connection
        redis_hook = RedisHook(redis_conn_id='redis_default')
        redis_conn = redis_hook.get_conn()
        
        processed_count = 0
        for message in consumer:
            record = message.value
            
            # Cache in Redis with TTL
            cache_key = f"event:{record['id']}"
            redis_conn.setex(cache_key, 3600, json.dumps(record))  # 1 hour TTL
            
            # Also maintain a list of recent events
            redis_conn.lpush('recent_events', json.dumps(record))
            redis_conn.ltrim('recent_events', 0, 999)  # Keep only last 1000 events
            
            processed_count += 1
            logging.info(f"Cached record in Redis: {record['id']}")
        
        consumer.close()
        
        logging.info(f"Successfully processed {processed_count} records from Kafka to Redis")
        return f"Processed {processed_count} records"
        
    except Exception as e:
        logging.error(f"Error consuming from Kafka to Redis: {str(e)}")
        raise

def aggregate_and_store_postgres(**context):
    """Aggregate data and store in PostgreSQL"""
    try:
        # Initialize Redis connection
        redis_hook = RedisHook(redis_conn_id='redis_default')
        redis_conn = redis_hook.get_conn()
        
        # Get recent events from Redis
        recent_events_raw = redis_conn.lrange('recent_events', 0, -1)
        recent_events = [json.loads(event.decode('utf-8')) for event in recent_events_raw]
        
        if not recent_events:
            logging.info("No events found in Redis")
            return "No events to process"
        
        # Convert to DataFrame for aggregation
        df = pd.DataFrame(recent_events)
        
        # Perform aggregations
        event_counts = df['event_type'].value_counts().to_dict()
        avg_value_by_event = df.groupby('event_type')['value'].mean().to_dict()
        
        # Initialize PostgreSQL connection
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Insert aggregated data
        for event_type, count in event_counts.items():
            avg_value = avg_value_by_event.get(event_type, 0)
            
            insert_sql = """
                INSERT INTO event_aggregations (event_type, event_count, avg_value, aggregation_timestamp)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (event_type, aggregation_timestamp) 
                DO UPDATE SET event_count = EXCLUDED.event_count, avg_value = EXCLUDED.avg_value
            """
            
            postgres_hook.run(
                insert_sql,
                parameters=(event_type, count, avg_value, datetime.now())
            )
        
        logging.info(f"Successfully stored aggregations for {len(event_counts)} event types")
        return f"Stored aggregations for {len(event_counts)} event types"
        
    except Exception as e:
        logging.error(f"Error aggregating and storing in PostgreSQL: {str(e)}")
        raise

def cleanup_old_data(**context):
    """Clean up old data from Redis and PostgreSQL"""
    try:
        # Clean old aggregations (keep only last 30 days)
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        cleanup_sql = """
            DELETE FROM event_aggregations 
            WHERE aggregation_timestamp < NOW() - INTERVAL '30 days'
        """
        postgres_hook.run(cleanup_sql)
        
        logging.info("Successfully cleaned up old data")
        return "Cleanup completed"
        
    except Exception as e:
        logging.error(f"Error during cleanup: {str(e)}")
        raise

# Create table for aggregations
create_table_task = PostgresOperator(
    task_id='create_aggregation_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS event_aggregations (
            id SERIAL PRIMARY KEY,
            event_type VARCHAR(50) NOT NULL,
            event_count INTEGER NOT NULL,
            avg_value DECIMAL(10,2),
            aggregation_timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(event_type, aggregation_timestamp)
        );
        
        CREATE INDEX IF NOT EXISTS idx_event_aggregations_timestamp 
        ON event_aggregations(aggregation_timestamp);
    """,
    dag=dag,
)

# Task to generate sample data
generate_data_task = PythonOperator(
    task_id='generate_sample_data',
    python_callable=create_sample_data,
    dag=dag,
)

# Task to produce data to Kafka
kafka_producer_task = PythonOperator(
    task_id='produce_to_kafka',
    python_callable=produce_to_kafka,
    dag=dag,
)

# Task to consume from Kafka and cache in Redis
kafka_to_redis_task = PythonOperator(
    task_id='consume_kafka_to_redis',
    python_callable=consume_from_kafka_to_redis,
    dag=dag,
)

# Task to aggregate and store in PostgreSQL
redis_to_postgres_task = PythonOperator(
    task_id='aggregate_to_postgres',
    python_callable=aggregate_and_store_postgres,
    dag=dag,
)

# Task to cleanup old data
cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag,
)

# Define task dependencies
create_table_task >> generate_data_task >> kafka_producer_task >> kafka_to_redis_task >> redis_to_postgres_task >> cleanup_task