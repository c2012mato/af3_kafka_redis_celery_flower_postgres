-- Initialize the streaming pipeline database schema

-- Create database for streaming data (if not exists)
CREATE DATABASE IF NOT EXISTS streaming_data;

-- Connect to the airflow database and create necessary extensions
\c airflow;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create schema for streaming pipeline
CREATE SCHEMA IF NOT EXISTS streaming;

-- Event aggregations table
CREATE TABLE IF NOT EXISTS streaming.event_aggregations (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    event_count INTEGER NOT NULL,
    avg_value DECIMAL(10,2),
    aggregation_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(event_type, aggregation_timestamp)
);

-- Raw events table for archival
CREATE TABLE IF NOT EXISTS streaming.raw_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_id VARCHAR(100) NOT NULL,
    user_id INTEGER,
    event_type VARCHAR(50) NOT NULL,
    event_value DECIMAL(10,2),
    metadata JSONB,
    event_timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User activity summary table
CREATE TABLE IF NOT EXISTS streaming.user_activity_summary (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    total_events INTEGER DEFAULT 0,
    last_activity TIMESTAMP,
    total_value DECIMAL(12,2) DEFAULT 0,
    activity_summary JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id)
);

-- System metrics table
CREATE TABLE IF NOT EXISTS streaming.system_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,2),
    metric_tags JSONB,
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Error logs table
CREATE TABLE IF NOT EXISTS streaming.error_logs (
    id SERIAL PRIMARY KEY,
    error_source VARCHAR(100) NOT NULL,
    error_message TEXT,
    error_details JSONB,
    severity VARCHAR(20) DEFAULT 'ERROR',
    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_event_aggregations_timestamp ON streaming.event_aggregations(aggregation_timestamp);
CREATE INDEX IF NOT EXISTS idx_event_aggregations_type ON streaming.event_aggregations(event_type);

CREATE INDEX IF NOT EXISTS idx_raw_events_timestamp ON streaming.raw_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_events_type ON streaming.raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_user ON streaming.raw_events(user_id);

CREATE INDEX IF NOT EXISTS idx_user_activity_user ON streaming.user_activity_summary(user_id);
CREATE INDEX IF NOT EXISTS idx_user_activity_last ON streaming.user_activity_summary(last_activity);

CREATE INDEX IF NOT EXISTS idx_system_metrics_name ON streaming.system_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_system_metrics_recorded ON streaming.system_metrics(recorded_at);

CREATE INDEX IF NOT EXISTS idx_error_logs_source ON streaming.error_logs(error_source);
CREATE INDEX IF NOT EXISTS idx_error_logs_occurred ON streaming.error_logs(occurred_at);

-- Create views for common queries
CREATE OR REPLACE VIEW streaming.daily_event_summary AS
SELECT 
    DATE(aggregation_timestamp) as event_date,
    event_type,
    SUM(event_count) as total_events,
    AVG(avg_value) as avg_event_value
FROM streaming.event_aggregations
GROUP BY DATE(aggregation_timestamp), event_type
ORDER BY event_date DESC, total_events DESC;

CREATE OR REPLACE VIEW streaming.active_users_today AS
SELECT 
    user_id,
    total_events,
    last_activity,
    total_value
FROM streaming.user_activity_summary
WHERE last_activity >= CURRENT_DATE
ORDER BY total_events DESC;

-- Create function to update user activity
CREATE OR REPLACE FUNCTION streaming.update_user_activity()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO streaming.user_activity_summary (user_id, total_events, last_activity, total_value, activity_summary)
    VALUES (
        NEW.user_id, 
        1, 
        NEW.event_timestamp, 
        COALESCE(NEW.event_value, 0),
        jsonb_build_object('event_types', jsonb_build_object(NEW.event_type, 1))
    )
    ON CONFLICT (user_id) DO UPDATE SET
        total_events = streaming.user_activity_summary.total_events + 1,
        last_activity = GREATEST(streaming.user_activity_summary.last_activity, NEW.event_timestamp),
        total_value = streaming.user_activity_summary.total_value + COALESCE(NEW.event_value, 0),
        activity_summary = jsonb_set(
            COALESCE(streaming.user_activity_summary.activity_summary, '{}'),
            ARRAY['event_types', NEW.event_type],
            (COALESCE((streaming.user_activity_summary.activity_summary->'event_types'->NEW.event_type)::int, 0) + 1)::text::jsonb
        ),
        updated_at = CURRENT_TIMESTAMP;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update user activity
CREATE TRIGGER trigger_update_user_activity
    AFTER INSERT ON streaming.raw_events
    FOR EACH ROW
    EXECUTE FUNCTION streaming.update_user_activity();

-- Create function to clean old data
CREATE OR REPLACE FUNCTION streaming.cleanup_old_data(days_to_keep INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    -- Clean old aggregations
    DELETE FROM streaming.event_aggregations 
    WHERE aggregation_timestamp < CURRENT_TIMESTAMP - INTERVAL '1 day' * days_to_keep;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- Clean old raw events (keep more for archival)
    DELETE FROM streaming.raw_events 
    WHERE processed_at < CURRENT_TIMESTAMP - INTERVAL '1 day' * (days_to_keep * 3);
    
    -- Clean old system metrics
    DELETE FROM streaming.system_metrics 
    WHERE recorded_at < CURRENT_TIMESTAMP - INTERVAL '1 day' * days_to_keep;
    
    -- Clean old error logs
    DELETE FROM streaming.error_logs 
    WHERE occurred_at < CURRENT_TIMESTAMP - INTERVAL '1 day' * days_to_keep;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Insert some initial system metrics
INSERT INTO streaming.system_metrics (metric_name, metric_value, metric_tags) VALUES
('system.initialized', 1, '{"component": "database", "version": "1.0"}'),
('tables.created', 5, '{"schema": "streaming"}');

-- Grant permissions to airflow user
GRANT ALL PRIVILEGES ON SCHEMA streaming TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA streaming TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA streaming TO airflow;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA streaming TO airflow;