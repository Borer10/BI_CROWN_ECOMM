select
  'Pedidos' as entidade,
  count(*) as total_registros,
  max(updated_at) as ultima_atualizacao_dados
from public.orders

union all

select
  'Itens de Pedido' as entidade,
  count(*) as total_registros,
  max(updated_db_at) as ultima_atualizacao_dados
from public.order_items

union all

select
  'Clientes' as entidade,
  count(*) as total_registros,
  max(updated_at) as ultima_atualizacao_dados
from public.customers

union all

select
  'Checkpoint Orders' as entidade,
  null as total_registros,
  checkpoint as ultima_atualizacao_dados
from public.etl_state
where pipeline = 'shopify_orders'

union all

select
  'Checkpoint Customers' as entidade,
  null as total_registros,
  checkpoint as ultima_atualizacao_dados
from public.etl_state
where pipeline = 'shopify_customers'
order by entidade;