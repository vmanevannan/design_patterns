WITH

prod_table as (
    SELECT *, <column_name> AS primary_key FROM <prod_table_name>
),

dev_table as (
    SELECT *, <column_name> AS primary_key FROM <dev_table_name>
),

prod_differences AS (
    SELECT * FROM prod_table
    EXCEPT
    SELECT * FROM dev_table
),

dev_differences AS (
    SELECT * FROM dev_table
    EXCEPT
    SELECT * FROM prod_table
),

differences AS(
    SELECT * FROM prod_differences
    UNION ALL
    SELECT * FROM dev_differences
),

unioned_differences AS (
    SELECT 'prod' AS source, *
    FROM prod_table WHERE primary_key IN (SELECT primary_key FROM differences)
    UNION ALL
    SELECT 'dev' AS source, *
    FROM dev_table WHERE primary_key IN (SELECT primary_key FROM differences)
)

SELECT *
FROM unioned_differences
ORDER BY primary_key ASC, source
