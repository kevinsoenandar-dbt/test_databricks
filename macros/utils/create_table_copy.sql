{% macro create_table_copy(should_run=var("should_run", False)) %}

    {% if should_run and env_var("DBT_CLOUD_INVOCATION_CONTEXT") == "ci" %}
        {# Step 1 - Get the list of models changed based on the node selection methods #}
        {% set changed_models = [] %}
        {% for node in selected_resources %}
            {% if modules.re.search("^model\..+", node) %}
                {% do changed_models.append(node) %}
            {% endif %}
        {% endfor %}

        {# Step 2 - Execute the create table statement for each model #}
        {% for model in changed_models %}
            {% set node_object = graph.nodes.values() | selectattr("unique_id", "equalto", model) | first %}
            {% do log(tables_to_copy, info=True) %}
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