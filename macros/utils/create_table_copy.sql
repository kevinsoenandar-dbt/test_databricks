{% macro create_table_copy(should_run=var("should_run", False)) %}

    {% if should_run and env_var("DBT_CLOUD_INVOCATION_CONTEXT") == "ci" %}

        {% for node in selected_resources %}
            {% set node_object = graph.nodes.values() | selectattr("resource_type", "equalto", "model") | selectattr("unique_id", "equalto", node) | first %}
            {% if node_object | length > 0 %}
                {% do _execute_create_table_query(node_object.database, node_object.schema, node_object.name) %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro _execute_create_table_query(database, schema, table_name) %}

    {% set create_table_query %}
    create table {{ table_name }} like {{ database }}.{{ schema }}.{{ table_name }}
    {% endset %}

    {% do run_query(create_table_query) %}
{% endmacro %}