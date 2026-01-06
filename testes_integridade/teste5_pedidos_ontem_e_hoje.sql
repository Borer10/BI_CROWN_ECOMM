select
  (created_at at time zone 'America/Sao_Paulo')::date as dia_sp,
  count(*) as pedidos
from public.orders
where created_at >= (now() at time zone 'America/Sao_Paulo') - interval '7 days'
group by 1
order by 1 desc;