"""
Custom Kafka operators for Airflow
"""
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.redis.hooks.redis_hook import RedisHook
import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class KafkaProducerOperator(BaseOperator):
    """
    Operator to send messages to a Kafka topic
    """
    
    template_fields = ['topic', 'messages']
    ui_color = '#FF6B6B'
    
    @apply_defaults
    def __init__(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        kafka_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.messages = messages
        self.kafka_config = kafka_config or {
            'bootstrap_servers': ['kafka:29092'],
            'value_serializer': lambda v: json.dumps(v).encode('utf-8')
        }
    
    def execute(self, context):
        producer = KafkaProducer(**self.kafka_config)
        
        try:
            sent_count = 0
            for message in self.messages:
                future = producer.send(self.topic, message)
                future.get(timeout=10)  # Wait for confirmation
                sent_count += 1
                logger.info(f"Sent message {sent_count} to topic {self.topic}")
            
            producer.flush()
            logger.info(f"Successfully sent {sent_count} messages to {self.topic}")
            return sent_count
            
        except Exception as e:
            logger.error(f"Failed to send messages to Kafka: {str(e)}")
            raise
        finally:
            producer.close()

class KafkaConsumerOperator(BaseOperator):
    """
    Operator to consume messages from a Kafka topic
    """
    
    template_fields = ['topics']
    ui_color = '#4ECDC4'
    
    @apply_defaults
    def __init__(
        self,
        topics: List[str],
        max_messages: int = 100,
        timeout_ms: int = 10000,
        kafka_config: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.topics = topics
        self.max_messages = max_messages
        self.timeout_ms = timeout_ms
        self.kafka_config = kafka_config or {
            'bootstrap_servers': ['kafka:29092'],
            'auto_offset_reset': 'earliest',
            'consumer_timeout_ms': timeout_ms,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8'))
        }
    
    def execute(self, context):
        consumer = KafkaConsumer(*self.topics, **self.kafka_config)
        
        try:
            messages = []
            for message in consumer:
                messages.append({
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'value': message.value,
                    'timestamp': message.timestamp
                })
                
                if len(messages) >= self.max_messages:
                    break
            
            logger.info(f"Consumed {len(messages)} messages from topics {self.topics}")
            return messages
            
        except Exception as e:
            logger.error(f"Failed to consume messages from Kafka: {str(e)}")
            raise
        finally:
            consumer.close()

class RedisToPostgresOperator(BaseOperator):
    """
    Operator to transfer data from Redis to PostgreSQL
    """
    
    template_fields = ['redis_key_pattern', 'postgres_table']
    ui_color = '#45B7D1'
    
    @apply_defaults
    def __init__(
        self,
        redis_key_pattern: str,
        postgres_table: str,
        redis_conn_id: str = 'redis_default',
        postgres_conn_id: str = 'postgres_default',
        batch_size: int = 1000,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redis_key_pattern = redis_key_pattern
        self.postgres_table = postgres_table
        self.redis_conn_id = redis_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.batch_size = batch_size
    
    def execute(self, context):
        redis_hook = RedisHook(redis_conn_id=self.redis_conn_id)
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        redis_conn = redis_hook.get_conn()
        
        try:
            # Get keys matching pattern
            keys = redis_conn.keys(self.redis_key_pattern)
            logger.info(f"Found {len(keys)} keys matching pattern {self.redis_key_pattern}")
            
            processed_count = 0
            batch_data = []
            
            for key in keys:
                value = redis_conn.get(key)
                if value:
                    try:
                        data = json.loads(value.decode('utf-8'))
                        batch_data.append(data)
                        
                        if len(batch_data) >= self.batch_size:
                            self._insert_batch(postgres_hook, batch_data)
                            processed_count += len(batch_data)
                            batch_data = []
                            
                    except json.JSONDecodeError:
                        logger.warning(f"Skipping invalid JSON for key {key}")
            
            # Insert remaining data
            if batch_data:
                self._insert_batch(postgres_hook, batch_data)
                processed_count += len(batch_data)
            
            logger.info(f"Transferred {processed_count} records from Redis to PostgreSQL")
            return processed_count
            
        except Exception as e:
            logger.error(f"Failed to transfer data from Redis to PostgreSQL: {str(e)}")
            raise
    
    def _insert_batch(self, postgres_hook, batch_data):
        """Insert a batch of data into PostgreSQL"""
        if not batch_data:
            return
        
        # This is a simplified insert - in practice, you'd want to customize this
        # based on your table schema
        insert_sql = f"""
            INSERT INTO {self.postgres_table} (data, created_at)
            VALUES (%s, CURRENT_TIMESTAMP)
        """
        
        for data in batch_data:
            postgres_hook.run(insert_sql, parameters=(json.dumps(data),))

class DataQualityCheckOperator(BaseOperator):
    """
    Operator to perform data quality checks
    """
    
    template_fields = ['table_name', 'checks']
    ui_color = '#FFA07A'
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        checks: List[Dict[str, Any]],
        postgres_conn_id: str = 'postgres_default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.checks = checks
        self.postgres_conn_id = postgres_conn_id
    
    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        failed_checks = []
        
        for check in self.checks:
            check_name = check.get('name', 'Unnamed check')
            check_sql = check.get('sql')
            expected_result = check.get('expected_result')
            
            try:
                result = postgres_hook.get_first(check_sql)
                actual_result = result[0] if result else None
                
                if actual_result != expected_result:
                    failed_checks.append({
                        'name': check_name,
                        'expected': expected_result,
                        'actual': actual_result,
                        'sql': check_sql
                    })
                    logger.warning(f"Data quality check failed: {check_name}")
                else:
                    logger.info(f"Data quality check passed: {check_name}")
                    
            except Exception as e:
                failed_checks.append({
                    'name': check_name,
                    'error': str(e),
                    'sql': check_sql
                })
                logger.error(f"Error executing data quality check {check_name}: {str(e)}")
        
        if failed_checks:
            raise ValueError(f"Data quality checks failed: {failed_checks}")
        
        logger.info(f"All {len(self.checks)} data quality checks passed")
        return True