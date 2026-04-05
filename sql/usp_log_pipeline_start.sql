CREATE PROCEDURE usp_log_pipeline_start
    @pipeline_name  VARCHAR(200),
    @run_id         VARCHAR(200),
    @p_year         VARCHAR(4),
    @p_month        VARCHAR(2),
    @p_day          VARCHAR(2)
AS
BEGIN
    INSERT INTO pipeline_run_log
        (pipeline_name, run_id, p_year, p_month, p_day, status, start_time)
    VALUES
        (@pipeline_name, @run_id, @p_year, @p_month, @p_day, 'RUNNING', GETUTCDATE());
END;