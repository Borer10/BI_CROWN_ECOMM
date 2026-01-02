import os, time, sys
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime, timezone, timedelta
from pathlib import Path

load_dotenv()

# =========================
# LOCK FILE (anti concorrência)
# =========================
LOCK_FILE = Path("shopify_incremental.lock")

if LOCK_FILE.exists():
    print("⛔ ETL já está rodando. Saindo para evitar execução dupla.")
    sys.exit(0)

try:
    LOCK_FILE.touch()
except Exception as e:
    print(f"Erro ao criar lock file: {e}")
    sys.exit(1)

SHOP = os.getenv("SHOPIFY_SHOP")
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")
DB_URL = os.getenv("POSTGRES_URL")

# limite “amigável” para não estourar timeout do Actions
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))  # 0 = sem limite
START_TIME = time.time()

if not all([SHOP, TOKEN, DB_URL]):
    raise SystemExit("Faltou SHOPIFY_SHOP / SHOPIFY_ADMIN_TOKEN / POSTGRES_URL no .env")

URL = f"https://{SHOP}/admin/api/{VERSION}/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

# =========================
# STATE NO POSTGRES (Supabase)
# =========================
PIPELINE_NAME = "shopify_orders"
DEFAULT_START = "2025-01-01T00:00:00Z"
SAFETY_WINDOW = timedelta(hours=2)

engine = create_engine(DB_URL, pool_pre_ping=True)

STATE_TABLE_DDL = text("""
create table if not exists etl_state (
  pipeline text primary key,
  checkpoint timestamptz not null,
  updated_at timestamptz not null default now()
);
""")

GET_STATE = text("""
select checkpoint
from etl_state
where pipeline = :pipeline;
""")

UPSERT_STATE = text("""
insert into etl_state (pipeline, checkpoint, updated_at)
values (:pipeline, :checkpoint, now())
on conflict (pipeline) do update
set checkpoint = excluded.checkpoint,
    updated_at = now();
""")

def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def gql(variables):
    r = requests.post(URL, headers=HEADERS, json={"query": QUERY, "variables": variables}, timeout=60)
    r.raise_for_status()
    j = r.json()
    if "errors" in j:
        raise RuntimeError(j["errors"])
    return j["data"]

def money(set_obj):
    try:
        return float(set_obj["shopMoney"]["amount"])
    except Exception:
        return None

def should_stop_soon():
    if MAX_RUNTIME_MIN <= 0:
        return False
    elapsed_min = (time.time() - START_TIME) / 60.0
    return elapsed_min >= MAX_RUNTIME_MIN

# =========================
# SHOPIFY QUERY
# =========================
QUERY = """
query Orders($first: Int!, $after: String, $query: String) {
  orders(first: $first, after: $after, query: $query, sortKey: UPDATED_AT, reverse: false) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        name
        createdAt
        updatedAt
        displayFinancialStatus
        displayFulfillmentStatus
        currencyCode
        totalPriceSet { shopMoney { amount currencyCode } }
        customer { id email }
        lineItems(first: 250) {
          edges {
            node {
              id
              name
              quantity
              sku
              originalUnitPriceSet { shopMoney { amount currencyCode } }
            }
          }
        }
      }
    }
  }
}
"""

# =========================
# UPSERTS
# =========================
UPSERT_ORDER = text("""
insert into public.orders (
  order_id, order_name, created_at, updated_at, financial_status, fulfillment_status,
  currency, total_price, customer_id, customer_email, updated_db_at
) values (
  :order_id, :order_name, :created_at, :updated_at, :financial_status, :fulfillment_status,
  :currency, :total_price, :customer_id, :customer_email, now()
)
on conflict (order_id) do update set
  order_name = excluded.order_name,
  created_at = excluded.created_at,
  updated_at = excluded.updated_at,
  financial_status = excluded.financial_status,
  fulfillment_status = excluded.fulfillment_status,
  currency = excluded.currency,
  total_price = excluded.total_price,
  customer_id = excluded.customer_id,
  customer_email = excluded.customer_email,
  updated_db_at = now();
""")

UPSERT_ITEM = text("""
insert into public.order_items (
  line_item_id, order_id, order_updated_at, sku, name, quantity, unit_price, currency, updated_db_at
) values (
  :line_item_id, :order_id, :order_updated_at, :sku, :name, :quantity, :unit_price, :currency, now()
)
on conflict (line_item_id) do update set
  order_id = excluded.order_id,
  order_updated_at = excluded.order_updated_at,
  sku = excluded.sku,
  name = excluded.name,
  quantity = excluded.quantity,
  unit_price = excluded.unit_price,
  currency = excluded.currency,
  updated_db_at = now();
""")

def main():
    orders_count = 0
    items_count = 0

    with engine.connect() as conn:
        # ✅ tudo que mexe no DB precisa estar dentro de begin() para não deixar autobegin pendurado
        with conn.begin():
            conn.execute(STATE_TABLE_DDL)
            row = conn.execute(GET_STATE, {"pipeline": PIPELINE_NAME}).fetchone()
            if not row:
                checkpoint_dt = datetime.fromisoformat(DEFAULT_START.replace("Z", "+00:00"))
                conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt})
            else:
                checkpoint_dt = row[0]

        since
