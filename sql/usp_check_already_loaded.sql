CREATE PROCEDURE usp_check_already_loaded
    @p_year     VARCHAR(4),
    @p_month    VARCHAR(2),
    @p_day      VARCHAR(2),
    @is_loaded  BIT OUTPUT
AS
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pipeline_run_log
        WHERE p_year  = @p_year
        AND   p_month = @p_month
        AND   status  = 'SUCCESS'
    )
        SET @is_loaded = 1;
    ELSE
        SET @is_loaded = 0;
END;