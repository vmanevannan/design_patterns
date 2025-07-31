{% macro table_diff_comparison(
    prod_table, 
    dev_table, 
    primary_key_col,
    exclude_columns=[],
    prod_source_name='prod',
    dev_source_name='dev'
) %}

{%- set columns_query -%}
    SELECT column_name 
    FROM {{ information_schema.columns }}
    WHERE table_name = '{{ prod_table }}'
    AND column_name NOT IN ({{ "'" + exclude_columns | join("','") + "'" if exclude_columns else "''" }})
{%- endset -%}

{%- set results = run_query(columns_query) -%}
{%- set column_names = results.columns[0].values() -%}

WITH
prod_table as (
    SELECT 
        {%- for col in column_names %}
        {{ col }}{{ "," if not loop.last }}
        {%- endfor %},
        {{ primary_key_col }} AS primary_key 
    FROM {{ ref(prod_table) }}
),

dev_table as (
    SELECT 
        {%- for col in column_names %}
        {{ col }}{{ "," if not loop.last }}
        {%- endfor %},
        {{ primary_key_col }} AS primary_key 
    FROM {{ ref(dev_table) }}
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
    SELECT '{{ prod_source_name }}' AS source, *
    FROM prod_table WHERE primary_key IN (SELECT primary_key FROM differences)
    UNION ALL
    SELECT '{{ dev_source_name }}' AS source, *
    FROM dev_table WHERE primary_key IN (SELECT primary_key FROM differences)
)

SELECT *
FROM unioned_differences
ORDER BY primary_key ASC, source

{% endmacro %}

-- models/user_table_diff.sql
{{ config(
    materialized='table',
    tags=['data_quality', 'comparison']
) }}

{{ table_diff_comparison(
    prod_table='users_prod', 
    dev_table='users_dev', 
    primary_key_col='user_id',
    exclude_columns=['created_at', 'updated_at'],
    prod_source_name='production',
    dev_source_name='development'
) }}

-- models/product_table_diff.sql  
{{ config(
    materialized='view',
    tags=['data_quality']
) }}

{{ table_diff_comparison(
    prod_table='products_prod', 
    dev_table='products_dev', 
    primary_key_col='product_id'
) }}

-- Additional efficiency patterns:

-- 1. MACRO PARAMETERS (what we used above):
{{ table_diff_comparison(
    prod_table='users_prod', 
    dev_table='users_dev', 
    primary_key_col='user_id',
    prod_source_name='production',  -- macro parameter
    dev_source_name='development'   -- macro parameter
) }}

-- 2. DBT VARIABLES for environment flexibility:
-- dbt_project.yml:
# vars:
#   prod_schema: 'prod'
#   dev_schema: 'dev'
#   source_environment: 'production'

-- models/flexible_table_diff.sql
{{ table_diff_comparison(
    prod_table=var('prod_schema') ~ '.users', 
    dev_table=var('dev_schema') ~ '.users', 
    primary_key_col='user_id',
    prod_source_name=var('source_environment', 'prod')  -- dbt variable with fallback
) }}

-- 2. Create a generic test for automated comparison
-- macros/test_table_differences.sql
{% test table_differences(model, prod_table, dev_table, primary_key_col) %}
    
    {{ table_diff_comparison(prod_table, dev_table, primary_key_col) }}
    
{% endtest %}

-- Usage in schema.yml:
# models:
#   - name: users
#     tests:
#       - table_differences:
#           prod_table: users_prod
#           dev_table: users_dev  
#           primary_key_col: user_id
