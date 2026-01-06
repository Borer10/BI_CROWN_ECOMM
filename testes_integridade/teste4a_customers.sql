-- 4) COBERTURA DE CUSTOMER NOS PEDIDOS
select
  count(*) as orders_total,
  count(*) filter (where customer_id is not null) as orders_com_customer_id,
  count(*) filter (where customer_email is not null and customer_email <> '') as orders_com_customer_email,
  round(100.0 * count(*) filter (where customer_id is not null) / nullif(count(*),0), 2) as pct_com_customer_id,
  round(100.0 * count(*) filter (where customer_email is not null and customer_email <> '') / nullif(count(*),0), 2) as pct_com_customer_email
from public.orders;