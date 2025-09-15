"""
Airflow 3.0.0 SDK-based streaming pipeline with Kafka, Redis, and PostgreSQL
Using modern decorators and optimized operators
"""
from datetime import datetime, timedelta
import json
import logging
import random
from typing import Any, Dict, List

import pandas as pd
from kafka import KafkaProducer, KafkaConsumer

# Import Airflow 3.0.0 SDK components
try:
    from airflow.sdk import DAG, Asset
    from airflow.sdk.decorators import task, dag
    from airflow.sdk.definitions.asset import Asset
except ImportError:
    # Fallback for compatibility
    from airflow import DAG
    from airflow.decorators import task, dag

# Import optimized operators for Airflow 3.0.0
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis_hook import RedisHook

# Define data assets for lineage tracking
KAFKA_EVENTS_ASSET = Asset("kafka://streaming_events")
REDIS_CACHE_ASSET = Asset("redis://event_cache") 
POSTGRES_AGGREGATIONS_ASSET = Asset("postgres://event_aggregations")

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='streaming_pipeline_sdk',
    default_args=default_args,
    description='Airflow 3.0.0 SDK streaming pipeline with modern patterns',
    schedule='0 */30 * * *',  # Every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['streaming', 'kafka', 'redis', 'postgres', 'airflow-3.0', 'sdk'],
    max_active_runs=1,
)
def streaming_pipeline_sdk():
    """
    Modern streaming pipeline DAG using Airflow 3.0.0 SDK patterns
    """
    
    # Task to create database table (non-decorated for SQL operations)
    create_table = PostgresOperator(
        task_id='create_event_aggregations_table',
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
    )
    
    @task(outlets=[KAFKA_EVENTS_ASSET])
    def generate_sample_data() -> List[Dict[str, Any]]:
        """Generate sample streaming data using modern task decorator"""
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
        
        logging.info(f"Generated {len(data)} sample records")
        return data

    @task(inlets=[KAFKA_EVENTS_ASSET])
    def produce_to_kafka(sample_data: List[Dict[str, Any]]) -> str:
        """Produce data to Kafka using optimized patterns"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:29092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Optimized settings for Airflow 3.0.0
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
            )
            
            sent_count = 0
            for record in sample_data:
                future = producer.send('streaming_events', record)
                future.get(timeout=10)
                sent_count += 1
            
            producer.flush()
            producer.close()
            
            message = f"Successfully produced {sent_count} events to Kafka"
            logging.info(message)
            return message
            
        except Exception as e:
            error_msg = f"Error producing to Kafka: {str(e)}"
            logging.error(error_msg)
            raise

    @task(inlets=[KAFKA_EVENTS_ASSET], outlets=[REDIS_CACHE_ASSET])
    def consume_kafka_to_redis() -> str:
        """Consume from Kafka and cache in Redis with optimized patterns"""
        try:
            # Initialize Redis connection
            redis_hook = RedisHook(redis_conn_id='redis_default')
            redis_conn = redis_hook.get_conn()
            
            # Configure optimized Kafka consumer
            consumer = KafkaConsumer(
                'streaming_events',
                bootstrap_servers=['kafka:29092'],
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                consumer_timeout_ms=30000,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='airflow_consumer_group'
            )
            
            consumed_count = 0
            events_batch = []
            
            # Consume messages with timeout
            for message in consumer:
                event_data = message.value
                events_batch.append(event_data)
                consumed_count += 1
                
                # Batch processing for better performance
                if len(events_batch) >= 50:
                    for event in events_batch:
                        redis_conn.lpush('recent_events', json.dumps(event))
                    events_batch = []
            
            # Process remaining events
            if events_batch:
                for event in events_batch:
                    redis_conn.lpush('recent_events', json.dumps(event))
            
            # Maintain only recent events (last 1000)
            redis_conn.ltrim('recent_events', 0, 999)
            
            consumer.close()
            
            message = f"Successfully consumed {consumed_count} events to Redis"
            logging.info(message)
            return message
            
        except Exception as e:
            error_msg = f"Error consuming from Kafka to Redis: {str(e)}"
            logging.error(error_msg)
            raise

    @task(inlets=[REDIS_CACHE_ASSET], outlets=[POSTGRES_AGGREGATIONS_ASSET])
    def aggregate_to_postgres() -> str:
        """Aggregate data and store in PostgreSQL using modern patterns"""
        try:
            # Initialize connections
            redis_hook = RedisHook(redis_conn_id='redis_default')
            redis_conn = redis_hook.get_conn()
            postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # Get recent events from Redis
            recent_events_raw = redis_conn.lrange('recent_events', 0, -1)
            
            if not recent_events_raw:
                logging.info("No events found in Redis")
                return "No events to process"
            
            recent_events = [json.loads(event.decode('utf-8')) for event in recent_events_raw]
            
            # Use pandas for efficient aggregation
            df = pd.DataFrame(recent_events)
            
            # Perform aggregations
            event_counts = df['event_type'].value_counts().to_dict()
            avg_value_by_event = df.groupby('event_type')['value'].mean().to_dict()
            
            # Batch insert for better performance
            batch_data = []
            for event_type, count in event_counts.items():
                avg_value = avg_value_by_event.get(event_type, 0)
                batch_data.append((event_type, count, avg_value, datetime.now()))
            
            # Use efficient batch insert
            insert_sql = """
                INSERT INTO event_aggregations (event_type, event_count, avg_value, aggregation_timestamp)
                VALUES %s
                ON CONFLICT (event_type, aggregation_timestamp) 
                DO UPDATE SET event_count = EXCLUDED.event_count, avg_value = EXCLUDED.avg_value
            """
            
            # Execute batch insert
            postgres_hook.run(
                "INSERT INTO event_aggregations (event_type, event_count, avg_value, aggregation_timestamp) VALUES " +
                ",".join([f"('{data[0]}', {data[1]}, {data[2]}, '{data[3]}')" for data in batch_data]) +
                " ON CONFLICT (event_type, aggregation_timestamp) DO UPDATE SET event_count = EXCLUDED.event_count, avg_value = EXCLUDED.avg_value"
            )
            
            message = f"Successfully stored aggregations for {len(event_counts)} event types"
            logging.info(message)
            return message
            
        except Exception as e:
            error_msg = f"Error aggregating and storing in PostgreSQL: {str(e)}"
            logging.error(error_msg)
            raise

    @task
    def cleanup_old_data() -> str:
        """Clean up old data with optimized patterns"""
        try:
            postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
            
            # Clean old aggregations (keep only last 30 days)
            cleanup_sql = """
                DELETE FROM event_aggregations 
                WHERE aggregation_timestamp < NOW() - INTERVAL '30 days'
            """
            result = postgres_hook.run(cleanup_sql)
            
            message = "Successfully cleaned up old data"
            logging.info(message)
            return message
            
        except Exception as e:
            error_msg = f"Error during cleanup: {str(e)}"
            logging.error(error_msg)
            raise

    # Define task dependencies using modern patterns
    sample_data = generate_sample_data()
    kafka_result = produce_to_kafka(sample_data)
    redis_result = consume_kafka_to_redis()
    postgres_result = aggregate_to_postgres()
    cleanup_result = cleanup_old_data()
    
    # Chain dependencies with data lineage
    create_table >> sample_data >> kafka_result >> redis_result >> postgres_result >> cleanup_result

# Create the DAG instance
streaming_dag = streaming_pipeline_sdk()