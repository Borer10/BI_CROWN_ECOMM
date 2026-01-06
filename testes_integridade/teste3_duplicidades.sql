-- 3A) DUPLICIDADE DE ORDER_ID (deveria ser 0)
select order_id, count(*) as qtd
from public.orders
group by order_id
having count(*) > 1
order by qtd desc;

-- 3B) DUPLICIDADE DE LINE_ITEM_ID (deveria ser 0)
select line_item_id, count(*) as qtd
from public.order_items
group by line_item_id
having count(*) > 1
order by qtd desc;

-- 3C) DUPLICIDADE DE CUSTOMER_ID (deveria ser 0)
select customer_id, count(*) as qtd
from public.customers
group by customer_id
having count(*) > 1
order by qtd desc;