{%- macro decode_generic(column_name, decode_type = 'gender') -%}
    {%- if decode_type == 'gender' -%}
        case
            when {{ column_name }} = 'F' then 'Female'
            when {{ column_name }} = 'M' then 'Male'
            when {{ column_name }} = 'X' then 'Non-binary'
            else {{ column_name }}
        end
    {%- elif decode_type == 'order_status' -%}
        case
            when lower({{ column_name }}) = 'pending' then 'processing'
            when lower({{ column_name }}) = 'shipped' then 'processing'
            when lower({{ column_name }}) = 'delivered' then 'completed'
            else {{ column_name }}
        end
    {% endif %}
{% endmacro %}