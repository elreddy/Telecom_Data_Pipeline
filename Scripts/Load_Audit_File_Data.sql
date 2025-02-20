CREATE TABLE IF NOT EXISTS audit_files_log(
    file_name VARCHAR(300) NOT NULL UNIQUE,
    records_count INT NOT NULL,
    duplicates_count INT NOT NULL,
    final_count INT NOT NULL,
    processed_date DATE
);

CREATE TEMP table temp_audit_table AS TABLE audit_files_log WITH NO DATA;

DO $$ 
DECLARE 
    file_path TEXT;
    query TEXT;
BEGIN
    file_path := '/home/lokesh/Telecome_Pipeline/Audit/Audit_' || TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') || '.csv';
    query := FORMAT('COPY temp_audit_table(file_name, records_count, duplicates_count, final_count, processed_date) FROM %L WITH (FORMAT csv, HEADER true)', file_path);
    EXECUTE query;
END $$;

INSERT INTO audit_files_log
SELECT * FROM temp_audit_table
ON CONFLICT(file_name) DO NOTHING;
