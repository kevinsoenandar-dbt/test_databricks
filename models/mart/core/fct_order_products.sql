{{ config(
    materialized = 'incremental',
    incremental_strategy = 'microbatch', 
    event_time = 'order_date',
    begin = '2025-01-01',
    batch_size = 'day'
)
}}

with order_products as (
    select * 
    
    from {{ ref("stg_bike_shop__order_products") }}

    {% endif %}
)

, products as (
    select * from {{ ref("stg_bike_shop__products") }}
)

, orders as (
    select * from {{ ref("stg_bike_shop__orders") }}
)

, transformed as (
    select
        order_products.order_product_id,
        order_products.product_id,
        order_products.order_id,
        orders.customer_id,
        orders.order_date,
        order_products.order_product_quantity,
        products.product_price,
        products.product_cost

    from order_products

    left join products
        on order_products.product_id = products.product_id

    left join orders
        on order_products.order_id = orders.order_id
)

select * from transformed