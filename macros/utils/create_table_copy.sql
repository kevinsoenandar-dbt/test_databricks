{% macro create_table_copy(should_run=var("should_run", False)) %}

    {% if execute %}
        {% if should_run | as_bool and env_var("DBT_CLOUD_INVOCATION_CONTEXT") == "ci" %}

            {% do _create_temp_pr_schema() %}

            {% for node in selected_resources %}
                {% set node_object = graph.nodes.values() 
                    | selectattr("resource_type", "equalto", "model") 
                    | selectattr("config.materialized", "equalto", "incremental")
                    | selectattr("unique_id", "equalto", node) | first %}
                {% if node_object | length > 0 %}
                    {% set deferral_node = node_object.defer_relation.relation_name %}
                    {% if deferral_node %}
                        {% do log("Found deferral node of: " ~ deferral_node, info=True) %}
                        {% do _execute_create_table_query(node_object.name, deferral_node) %}
                    {% else %}
                        {% do log("No deferral node found for: " ~ node_object.name ~ ", skipping table copy creation...", info=True) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}

    {% endif %}
{% endmacro %}

{% macro _create_temp_pr_schema() %}
    {% set create_temp_pr_schema_query %}
    create schema if not exists {{ target.database }}.{{ target.schema }}
    {% endset %}

    {% do run_query(create_temp_pr_schema_query) %}
{% endmacro%}

{% macro _execute_create_table_query(table_name, defer_relation) %}

    {% set create_table_query %}
    create table {{ target.schema }}.{{ table_name }} like {{ defer_relation }}
    {% endset %}

    {% do run_query(create_table_query) %}
{% endmacro %}