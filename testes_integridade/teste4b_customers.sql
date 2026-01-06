-- 4B) PEDIDOS COM CUSTOMER_ID QUE NÃO EXISTE NA TABELA CUSTOMERS
select
  count(*) as orders_customer_id_sem_match
from public.orders o
left join public.customers c
  on c.customer_id = o.customer_id
where o.customer_id is not null
  and c.customer_id is null;