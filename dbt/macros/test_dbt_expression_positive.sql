{% test dbt_expression_positive(model, column_name) %}
select * from {{ model }} where {{ column_name }} is null or {{ column_name }} <= 0
{% endtest %}
