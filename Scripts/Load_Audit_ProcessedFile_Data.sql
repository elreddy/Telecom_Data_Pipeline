CREATE TABLE IF NOT EXISTS audit_processedfiles_log(
    file_name VARCHAR(300) NOT NULL UNIQUE,
    records_count INT NOT NULL,
    processed_date DATE
);

CREATE TEMP table temp_audit_table AS TABLE audit_processedfiles_log WITH NO DATA;

DO $$ 
DECLARE 
    file_path TEXT;
    query TEXT;
BEGIN
    file_path := '/home/lokesh/Telecome_Pipeline/Audit/Audit_Processedfiles_' || TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') || '.csv';
    query := FORMAT('COPY temp_audit_table(file_name, records_count, processed_date) FROM %L WITH (FORMAT csv, HEADER true)', file_path);
    EXECUTE query;
END $$;

INSERT INTO audit_processedfiles_log
SELECT * FROM temp_audit_table
ON CONFLICT(file_name) DO NOTHING;
