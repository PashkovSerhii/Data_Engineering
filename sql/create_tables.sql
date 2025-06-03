-- Скрипт створення всіх таблиць AdTech DB
USE adtech_db;

-- Створення таблиць у правильному порядку (з урахуванням залежностей)

CREATE TABLE advertisers (
    advertiser_id INT AUTO_INCREMENT PRIMARY KEY,
    advertiser_name VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_advertiser_name (advertiser_name)
);

CREATE TABLE campaigns (
    campaign_id INT AUTO_INCREMENT PRIMARY KEY,
    advertiser_id INT NOT NULL,
    campaign_name VARCHAR(200) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    targeting_criteria TEXT,
    ad_slot_size VARCHAR(20),
    budget DECIMAL(12,2) NOT NULL,
    remaining_budget DECIMAL(12,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (advertiser_id) REFERENCES advertisers(advertiser_id),
    INDEX idx_advertiser_campaign (advertiser_id, campaign_name),
    INDEX idx_dates (start_date, end_date),
    INDEX idx_budget (budget, remaining_budget)
);

CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    age INT NOT NULL,
    gender ENUM('Male', 'Female', 'Non-Binary') NOT NULL,
    location VARCHAR(50) NOT NULL,
    signup_date DATE NOT NULL,
    INDEX idx_demographics (age, gender, location),
    INDEX idx_signup_date (signup_date)
);

CREATE TABLE user_interests (
    user_id BIGINT,
    interest VARCHAR(50),
    PRIMARY KEY (user_id, interest),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE ad_events (
    event_id VARCHAR(36) PRIMARY KEY,
    campaign_id INT NOT NULL,
    user_id BIGINT NOT NULL,
    device ENUM('Mobile', 'Desktop', 'Tablet') NOT NULL,
    location VARCHAR(50) NOT NULL,
    timestamp DATETIME NOT NULL,
    bid_amount DECIMAL(8,2) NOT NULL,
    ad_cost DECIMAL(8,2) NOT NULL,
    ad_revenue DECIMAL(8,2) DEFAULT 0.00,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    INDEX idx_campaign_timestamp (campaign_id, timestamp),
    INDEX idx_user_device (user_id, device),
    INDEX idx_location_timestamp (location, timestamp),
    INDEX idx_costs (bid_amount, ad_cost)
);

CREATE TABLE clicks (
    click_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL,
    click_timestamp DATETIME NOT NULL,
    FOREIGN KEY (event_id) REFERENCES ad_events(event_id) ON DELETE CASCADE,
    INDEX idx_event_timestamp (event_id, click_timestamp)
);

SELECT 'Всі таблиці створені успішно!' as status;