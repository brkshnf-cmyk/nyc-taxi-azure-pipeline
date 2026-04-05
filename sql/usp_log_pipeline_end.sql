CREATE PROCEDURE usp_log_pipeline_end
    @run_id         VARCHAR(200),
    @status         VARCHAR(20),
    @files_written  BIGINT,
    @bytes_written  BIGINT,
    @error_message  VARCHAR(MAX)
AS
BEGIN
    UPDATE pipeline_run_log
    SET
        status        = @status,
        files_written = @files_written,
        bytes_written = @bytes_written,
        end_time      = GETUTCDATE(),
        error_message = @error_message
    WHERE run_id = @run_id;
END;