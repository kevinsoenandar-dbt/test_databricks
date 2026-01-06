{% macro create_table_copy(should_run=var("should_run", False)) %}

    {% if should_run and env_var("DBT_CLOUD_INVOCATION_CONTEXT") == "ci" %}

        {% for model in changed_models %}
            {% set node_object = graph.nodes.values() | selectattr("unique_id", "equalto", model) | selectattr("resource_type", "equalto", "model") | first %}
            {% do log(model, info=True) %}
            {% do _execute_create_table_query(node_object.database, node_object.schema, node_object.name) %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro _execute_create_table_query(database, schema, table_name) %}

    {% set create_table_query %}
    create table {{ table_name }} like {{ database }}.{{ schema }}.{{ table_name }}
    {% endset %}

    {% do run_query(create_table_query) %}
{% endmacro %}