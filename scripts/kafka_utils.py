"""
Kafka utilities for topic management and testing
"""
import json
import logging
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import random
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaManager:
    def __init__(self, bootstrap_servers=['kafka:29092']):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers,
            client_id='kafka_manager'
        )
    
    def create_topic(self, topic_name, num_partitions=3, replication_factor=1):
        """Create a new Kafka topic"""
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        try:
            self.admin_client.create_topics([topic])
            logger.info(f"Topic '{topic_name}' created successfully")
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{topic_name}' already exists")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {str(e)}")
    
    def list_topics(self):
        """List all Kafka topics"""
        try:
            metadata = self.admin_client.list_topics()
            return list(metadata)
        except Exception as e:
            logger.error(f"Failed to list topics: {str(e)}")
            return []
    
    def delete_topic(self, topic_name):
        """Delete a Kafka topic"""
        try:
            self.admin_client.delete_topics([topic_name])
            logger.info(f"Topic '{topic_name}' deleted successfully")
        except Exception as e:
            logger.error(f"Failed to delete topic '{topic_name}': {str(e)}")

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers=['kafka:29092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
    
    def send_message(self, topic, message, key=None):
        """Send a message to Kafka topic"""
        try:
            future = self.producer.send(topic, value=message, key=key)
            record_metadata = future.get(timeout=10)
            logger.info(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Failed to send message: {str(e)}")
            return False
    
    def send_batch_messages(self, topic, messages):
        """Send multiple messages to Kafka topic"""
        success_count = 0
        for message in messages:
            if self.send_message(topic, message):
                success_count += 1
        
        self.producer.flush()
        logger.info(f"Sent {success_count}/{len(messages)} messages successfully")
        return success_count
    
    def close(self):
        """Close the producer"""
        self.producer.close()

class KafkaConsumerWrapper:
    def __init__(self, topics, bootstrap_servers=['kafka:29092'], group_id='default_group'):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )
    
    def consume_messages(self, max_messages=None, timeout_ms=1000):
        """Consume messages from Kafka topics"""
        messages = []
        try:
            for message in self.consumer:
                messages.append({
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'key': message.key,
                    'value': message.value,
                    'timestamp': message.timestamp
                })
                
                if max_messages and len(messages) >= max_messages:
                    break
                
                # Simple timeout mechanism
                time.sleep(timeout_ms / 1000.0)
                
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
        
        return messages
    
    def close(self):
        """Close the consumer"""
        self.consumer.close()

def generate_sample_events(count=100):
    """Generate sample events for testing"""
    events = []
    event_types = ['click', 'view', 'purchase', 'signup', 'logout']
    
    for i in range(count):
        event = {
            'id': f"event_{i}",
            'timestamp': datetime.now().isoformat(),
            'user_id': random.randint(1, 1000),
            'event_type': random.choice(event_types),
            'value': round(random.uniform(10.0, 1000.0), 2),
            'metadata': {
                'source': random.choice(['web', 'mobile', 'api']),
                'device': random.choice(['mobile', 'desktop', 'tablet']),
                'location': random.choice(['US', 'EU', 'ASIA'])
            }
        }
        events.append(event)
    
    return events

def setup_kafka_topics():
    """Setup default Kafka topics for the streaming pipeline"""
    manager = KafkaManager()
    
    topics_to_create = [
        'streaming_events',
        'user_activities',
        'system_metrics',
        'error_logs',
        'processed_events'
    ]
    
    for topic in topics_to_create:
        manager.create_topic(topic, num_partitions=3, replication_factor=1)
    
    logger.info("Kafka topics setup completed")

if __name__ == "__main__":
    # Setup topics
    setup_kafka_topics()
    
    # Test producer
    producer = KafkaProducerWrapper()
    sample_events = generate_sample_events(10)
    producer.send_batch_messages('streaming_events', sample_events)
    producer.close()
    
    # Test consumer
    consumer = KafkaConsumerWrapper(['streaming_events'])
    messages = consumer.consume_messages(max_messages=5)
    logger.info(f"Consumed {len(messages)} messages")
    consumer.close()