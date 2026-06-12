select
    id as order_id,
    customer_id,
    amount,
    order_date
from {{ ref('orders') }}
