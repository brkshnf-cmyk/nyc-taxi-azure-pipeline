CREATE TABLE pipeline_run_log (
    log_id         INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name  VARCHAR(200),
    run_id         VARCHAR(200),
    p_year         VARCHAR(4),
    p_month        VARCHAR(2),
    p_day          VARCHAR(2),
    status         VARCHAR(20),
    files_written  BIGINT,
    bytes_written  BIGINT,
    start_time     DATETIME,
    end_time       DATETIME,
    error_message  VARCHAR(MAX)
);