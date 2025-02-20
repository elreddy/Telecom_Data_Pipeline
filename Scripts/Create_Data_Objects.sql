CREATE TABLE IF NOT EXISTS cdr_details(
    cdr_id SERIAL PRIMARY KEY,
    phone_number VARCHAR(50),
    account_length INT,
    vmail_message INT,
    day_mins DOUBLE PRECISION,
    day_charge DOUBLE PRECISION,
    eve_mins DOUBLE PRECISION,
    eve_charge DOUBLE PRECISION,
    night_mins DOUBLE PRECISION,
    night_charge DOUBLE PRECISION,
    intl_mins DOUBLE PRECISION,
    intl_charge DOUBLE PRECISION,
    is_fraud BOOLEAN,
    processing_date DATE 
);

CREATE TABLE IF NOT EXISTS cdr_activity(
    activity_id SERIAL PRIMARY KEY,
    phone_number VARCHAR(50),
    day_calls INT,
    eve_calls INT,
    night_calls INT,
    intl_calls INT,
    custserv_calls INT,
    total_charge DOUBLE PRECISION,
    processing_date DATE 
);

CREATE TABLE IF NOT EXISTS cdr_fraud_analysis (
    fraud_id SERIAL PRIMARY KEY,  
    phone_number VARCHAR(50),
    processing_date DATE,
    high_frequency BOOLEAN,
    short_intl_calls BOOLEAN,
    high_custserv_calls BOOLEAN,
    high_total_charge BOOLEAN,
    risk_level VARCHAR(20) CHECK (risk_level IN ('Low', 'Medium', 'High'))
);

