with order_products as (
    select * from {{ ref("fct_order_products") }}
)

select
    product_id,
    sum(order_product_quantity) as total_quantity,
    sum(product_price * order_product_quantity) as total_revenue,
    sum(product_cost * order_product_quantity) as total_cost,
    sum(product_price * order_product_quantity - product_cost * order_product_quantity) as total_profit

from order_products

group by 1