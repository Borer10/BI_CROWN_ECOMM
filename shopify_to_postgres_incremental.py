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

# =========================
# ENV
# =========================
SHOP = os.getenv("SHOPIFY_SHOP")
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")
DB_URL = os.getenv("POSTGRES_URL")

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

def ensure_state_table(conn):
    conn.execute(STATE_TABLE_DDL)

def get_checkpoint(conn) -> datetime:
    ensure_state_table(conn)
    row = conn.execute(GET_STATE, {"pipeline": PIPELINE_NAME}).fetchone()
    if not row:
        # fallback: início do projeto
        return datetime.fromisoformat(DEFAULT_START.replace("Z", "+00:00"))
    # timestamptz volta como datetime tz-aware
    return row[0]

def set_checkpoint(conn, checkpoint_dt: datetime):
    ensure_state_table(conn)
    conn.execute(
        UPSERT_STATE,
        {"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt}
    )

def iso_z(dt: datetime) -> str:
    # garante UTC com Z
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

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

    with engine.begin() as conn:
        # 1) checkpoint do Postgres
        checkpoint_dt = get_checkpoint(conn)

        # 2) janela de segurança de 2 horas
        since_dt = checkpoint_dt - SAFETY_WINDOW
        since_str = iso_z(since_dt)

        query_filter = f"updated_at:>={since_str}"

        after = None
        max_seen_updated_dt = checkpoint_dt  # dt
        max_seen_updated_str = iso_z(max_seen_updated_dt)  # string (para comparação/print)

        while True:
            data = gql({"first": 100, "after": after, "query": query_filter})
            edges = data["orders"]["edges"]

            for e in edges:
                o = e["node"]
                cust = o.get("customer") or {}

                # updatedAt vem em ISO 8601 com Z; convertemos para datetime UTC
                o_updated_dt = datetime.fromisoformat(o["updatedAt"].replace("Z", "+00:00"))

                if o_updated_dt > max_seen_updated_dt:
                    max_seen_updated_dt = o_updated_dt
                    max_seen_updated_str = iso_z(max_seen_updated_dt)

                conn.execute(UPSERT_ORDER, {
                    "order_id": o["id"],
                    "order_name": o["name"],
                    "created_at": o["createdAt"],
                    "updated_at": o["updatedAt"],
                    "financial_status": o.get("displayFinancialStatus"),
                    "fulfillment_status": o.get("displayFulfillmentStatus"),
                    "currency": o.get("currencyCode"),
                    "total_price": money(o.get("totalPriceSet") or {}),
                    "customer_id": cust.get("id"),
                    "customer_email": cust.get("email"),
                })
                orders_count += 1

                for li in ((o.get("lineItems") or {}).get("edges") or []):
                    n = li["node"]
                    conn.execute(UPSERT_ITEM, {
                        "line_item_id": n["id"],
                        "order_id": o["id"],
                        "order_updated_at": o["updatedAt"],
                        "sku": n.get("sku"),
                        "name": n.get("name"),
                        "quantity": n.get("quantity"),
                        "unit_price": money(n.get("originalUnitPriceSet") or {}),
                        "currency": (n.get("originalUnitPriceSet") or {}).get("shopMoney", {}).get("currencyCode"),
                    })
                    items_count += 1

            page = data["orders"]["pageInfo"]
            if not page["hasNextPage"]:
                break

            after = page["endCursor"]
            time.sleep(0.35)

        # 3) salva checkpoint no Postgres (o maior updatedAt visto)
        set_checkpoint(conn, max_seen_updated_dt)

    print(f"OK: upsert {orders_count} orders, {items_count} items. checkpoint={max_seen_updated_str}")

if __name__ == "__main__":
    try:
        main()
    finally:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()