"""
Celery configuration for Airflow
"""
from celery import Celery
import os

# Celery configuration
broker_url = os.getenv('AIRFLOW__CELERY__BROKER_URL', 'redis://:redis123@redis:6379/0')
result_backend = os.getenv('AIRFLOW__CELERY__RESULT_BACKEND', 'db+postgresql://airflow:airflow123@postgres:5432/airflow')

# Create Celery app
celery_app = Celery('airflow')

# Configure Celery
celery_app.conf.update(
    broker_url=broker_url,
    result_backend=result_backend,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_routes={
        'airflow.executors.celery_executor.execute_command': {'queue': 'default'},
    },
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_disable_rate_limits=False,
    task_compression='gzip',
    result_compression='gzip',
    worker_log_format='[%(asctime)s: %(levelname)s/%(processName)s] %(message)s',
    worker_task_log_format='[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s',
    
    # Task time limits
    task_soft_time_limit=600,  # 10 minutes
    task_time_limit=1200,      # 20 minutes
    
    # Task retry settings
    task_retry_delay=60,       # 1 minute
    task_max_retries=3,
    
    # Worker settings
    worker_max_tasks_per_child=1000,
    worker_max_memory_per_child=200000,  # 200MB
    
    # Monitoring
    worker_send_task_events=True,
    task_send_sent_event=True,
)