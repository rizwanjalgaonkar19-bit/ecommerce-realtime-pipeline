-- overall summary
select
    count(distinct order_id) as total_orders,
    round(sum(total_amount), 2) as total_revenue,
    round(sum(total_amount) / count(distinct order_id), 2) as avg_order_value
from orders
where order_status != 'canceled';

-- monthly sales
select
    strftime(order_date, '%Y-%m') as month,
    count(distinct order_id) as orders,
    round(sum(total_amount), 2) as revenue
from orders
where order_status != 'canceled'
group by month
order by month;

-- top 10 categories by revenue
select
    p.category,
    count(*) as items_sold,
    round(sum(o.total_amount), 2) as revenue
from orders o
join products p on o.product_id = p.product_id
where o.order_status != 'canceled'
group by p.category
order by revenue desc
limit 10;

-- orders by status
select order_status, count(distinct order_id) as orders
from orders
group by order_status
order by orders desc;

-- payment method share
select
    payment_method,
    count(distinct order_id) as orders,
    round(count(distinct order_id) * 100.0 / sum(count(distinct order_id)) over (), 2) as percent
from orders
group by payment_method
order by orders desc;

-- top 10 states by orders
select
    c.state,
    count(distinct o.order_id) as orders
from orders o
join customers c on o.customer_id = c.customer_id
group by c.state
order by orders desc
limit 10;

-- click funnel
select action, count(*) as events
from clicks
group by action
order by events desc;