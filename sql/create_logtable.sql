CREATE TABLE IF NOT EXISTS "LOGS".etl_logs (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(50) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP
)