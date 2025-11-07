-- ============================================
--   Initialize MySQL Database: forex_events
-- ============================================

SELECT '--- Initializing MySQL database: forex_events ---' AS Info;

-- Create database
CREATE DATABASE IF NOT EXISTS forex_events;
SELECT 'Database forex_events created (or already exists).' AS Info;
USE forex_events;

-- Main events table (keeping DATE type for proper date operations)
CREATE TABLE IF NOT EXISTS events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Date DATE NOT NULL,
    Time TIME NOT NULL,
    Currency VARCHAR(10) NOT NULL,
    Event VARCHAR(255) NOT NULL,
    Impact VARCHAR(20),
    Actual VARCHAR(50),
    Forecast VARCHAR(50),
    Previous VARCHAR(50)
);
SELECT 'Table events created (or already exists).' AS Info;

-- Create view for consistent date formatting
CREATE OR REPLACE VIEW events_formatted AS
SELECT 
    id,
    DATE_FORMAT(Date, '%e %M %Y') as Date,
    Time,
    Currency,
    Event,
    Impact,
    Actual,
    Forecast,
    Previous
FROM events;
SELECT 'View events_formatted created (or replaced).' AS Info;

-- Training metrics table
CREATE TABLE IF NOT EXISTS train_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Currency VARCHAR(10) NOT NULL,
    Event VARCHAR(255) NOT NULL,
    R2 FLOAT,
    MSE FLOAT,
    Samples INT
);
SELECT 'Table train_metrics created (or already exists).' AS Info;

-- Validation metrics table
CREATE TABLE IF NOT EXISTS validate_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Currency VARCHAR(10) NOT NULL,
    Event VARCHAR(255) NOT NULL,
    R2 FLOAT,
    MSE FLOAT,
    Samples INT
);
SELECT 'Table validate_metrics created (or already exists).' AS Info;

-- Test forecasts table 
CREATE TABLE IF NOT EXISTS test_forecasts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Currency VARCHAR(10) NOT NULL,
    Event VARCHAR(255) NOT NULL,
    R2 FLOAT,
    MSE FLOAT,
    Samples INT
);
SELECT 'Table test_forecasts created (or already exists).' AS Info;

-- Live forecasts table 
CREATE TABLE IF NOT EXISTS live_forecasts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Currency VARCHAR(10) NOT NULL,
    Event VARCHAR(255) NOT NULL,
    ForecastValue FLOAT
);
SELECT 'Table live_forecasts created (or already exists).' AS Info;

-- Secure user creation using caching_sha2_password
CREATE USER IF NOT EXISTS 'client_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'client_pass123';
GRANT SELECT, INSERT, UPDATE ON forex_events.* TO 'client_user'@'%';
FLUSH PRIVILEGES;

SELECT 'Client user created and privileges granted successfully.' AS Info;

SELECT '--- MySQL initialization complete ---' AS Info;
