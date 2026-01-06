-- 2) ITENS POR PEDIDO (busca anomalias)
select
  count(*) filter (where items_count = 0) as orders_sem_itens,
  count(*) filter (where items_count between 1 and 2) as orders_1a2_itens,
  count(*) filter (where items_count between 3 and 5) as orders_3a5_itens,
  count(*) filter (where items_count >= 6) as orders_6mais_itens,
  avg(items_count)::numeric(10,2) as media_itens_por_pedido
from (
  select
    o.order_id,
    coalesce(count(oi.line_item_id), 0) as items_count
  from public.orders o
  left join public.order_items oi
    on oi.order_id = o.order_id
  group by o.order_id
) t;