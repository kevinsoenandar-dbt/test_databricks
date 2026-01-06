{% macro create_table_copy(should_run=var("should_run", False)) %}

    {% if execute %}
        {% if should_run | as_bool and env_var("DBT_CLOUD_INVOCATION_CONTEXT") == "ci" %}
            {% do log(selected_resources, info=True) %}

            {% do _create_temp_pr_schema() %}

            {% for node in selected_resources %}
                {% set node_object = graph.nodes.values() 
                    | selectattr("resource_type", "equalto", "model") 
                    | selectattr("config.materialized", "equalto", "incremental")
                    | selectattr("unique_id", "equalto", node) | first %}
                {% if node_object | length > 0 %}
                    {% do _execute_create_table_query(node_object.database, node_object.schema, node_object.name) %}
                {% endif %}
            {% endfor %}
        {% endif %}

    {% endif %}
{% endmacro %}

{% macro _create_temp_pr_schema() %}
    {% set create_temp_pr_schema_query %}
    create schema if not exists {{ target.schema }}
    {% endset %}

    {% do run_query(create_temp_pr_schema_query) %}
{% endmacro%}

{% macro _execute_create_table_query(database, schema, table_name) %}

    {% set create_table_query %}
    create table {{ target.schema }}.{{ table_name }} like {{ database }}.{{ schema }}.{{ table_name }}
    {% endset %}

    {% do run_query(create_table_query) %}
{% endmacro %}