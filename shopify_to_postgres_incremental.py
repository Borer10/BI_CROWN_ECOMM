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

MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))  # 0 = sem limite
START_TIME = time.time()

BACKFILL_MODE = os.getenv("BACKFILL_MODE", "0") == "1"
BACKFILL_WINDOW_DAYS = int(os.getenv("BACKFILL_WINDOW_DAYS", "3"))

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
create table if not exists public.etl_state (
  pipeline text primary key,
  checkpoint timestamptz not null,
  updated_at timestamptz not null default now()
);
""")

GET_STATE = text("""
select checkpoint
from public.etl_state
where pipeline = :pipeline;
""")

UPSERT_STATE = text("""
insert into public.etl_state (pipeline, checkpoint, updated_at)
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

def parse_iso(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

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

def norm_str(v):
    # Normaliza strings (evita espaços) e retorna None se vier vazio
    if v is None:
        return None
    v = str(v).strip()
    return v if v else None

def norm_float(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None

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

        shippingAddress {
          city
          province
          country
          zip
          latitude
          longitude
        }

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
# OBS: Para este código funcionar, a tabela public.orders precisa ter as colunas:
# shipping_city, shipping_province, shipping_country, shipping_zip,
# shipping_latitude, shipping_longitude
UPSERT_ORDER = text("""
insert into public.orders (
  order_id, order_name, created_at, updated_at, financial_status, fulfillment_status,
  currency, total_price, customer_id, customer_email,
  shipping_city, shipping_province, shipping_country, shipping_zip,
  shipping_latitude, shipping_longitude,
  updated_db_at
) values (
  :order_id, :order_name, :created_at, :updated_at, :financial_status, :fulfillment_status,
  :currency, :total_price, :customer_id, :customer_email,
  :shipping_city, :shipping_province, :shipping_country, :shipping_zip,
  :shipping_latitude, :shipping_longitude,
  now()
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
  shipping_city = excluded.shipping_city,
  shipping_province = excluded.shipping_province,
  shipping_country = excluded.shipping_country,
  shipping_zip = excluded.shipping_zip,
  shipping_latitude = excluded.shipping_latitude,
  shipping_longitude = excluded.shipping_longitude,
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

def build_query_filter(checkpoint_dt: datetime):
    """
    Incremental:
      updated_at >= (checkpoint - safety)
    Backfill:
      updated_at >= (checkpoint - safety) AND updated_at < (checkpoint + window)
    """
    safe_start = checkpoint_dt - SAFETY_WINDOW
    default_start_dt = parse_iso(DEFAULT_START)
    if safe_start < default_start_dt:
        safe_start = default_start_dt

    start_str = iso_z(safe_start)

    if not BACKFILL_MODE:
        return f"updated_at:>={start_str}", None, safe_start

    window_end = checkpoint_dt + timedelta(days=BACKFILL_WINDOW_DAYS)
    end_str = iso_z(window_end)
    # Shopify search query aceita múltiplos filtros separados por espaço
    return f"updated_at:>={start_str} updated_at:<{end_str}", window_end, safe_start

def ensure_state_and_get_checkpoint(conn) -> datetime:
    conn.execute(STATE_TABLE_DDL)
    row = conn.execute(GET_STATE, {"pipeline": PIPELINE_NAME}).fetchone()
    if not row:
        checkpoint_dt = parse_iso(DEFAULT_START)
        conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt})
        return checkpoint_dt
    return row[0]

def update_checkpoint(conn, new_checkpoint_dt: datetime):
    conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": new_checkpoint_dt})

def main():
    orders_count = 0
    items_count = 0

    with engine.begin() as conn:
        checkpoint_dt = ensure_state_and_get_checkpoint(conn)

    query_filter, window_end_dt, safe_start_dt = build_query_filter(checkpoint_dt)

    mode_str = "BACKFILL" if BACKFILL_MODE else "INCREMENTAL"
    if BACKFILL_MODE:
        print(f"MODE={mode_str} checkpoint={iso_z(checkpoint_dt)} window_days={BACKFILL_WINDOW_DAYS} safety_start={iso_z(safe_start_dt)} end={iso_z(window_end_dt)}")
    else:
        print(f"MODE={mode_str} checkpoint={iso_z(checkpoint_dt)} safety_start={iso_z(safe_start_dt)}")

    after = None
    max_seen_updated_dt = checkpoint_dt  # vamos avançar com base no que vimos de fato
    stop_reason = None

    with engine.begin() as conn:
        while True:
            if should_stop_soon():
                stop_reason = "max_runtime"
                break

            data = gql({"first": 100, "after": after, "query": query_filter})
            edges = data["orders"]["edges"]

            # nada nessa página
            if not edges:
                page = data["orders"]["pageInfo"]
                if not page["hasNextPage"]:
                    break
                after = page["endCursor"]
                time.sleep(0.35)
                continue

            for e in edges:
                o = e["node"]
                cust = o.get("customer") or {}
                ship = o.get("shippingAddress") or {}

                o_updated_dt = parse_iso(o["updatedAt"])
                if o_updated_dt > max_seen_updated_dt:
                    max_seen_updated_dt = o_updated_dt

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

                    "shipping_city": norm_str(ship.get("city")),
                    "shipping_province": norm_str(ship.get("province")),
                    "shipping_country": norm_str(ship.get("country")),
                    "shipping_zip": norm_str(ship.get("zip")),
                    "shipping_latitude": norm_float(ship.get("latitude")),
                    "shipping_longitude": norm_float(ship.get("longitude")),
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

        # ✅ checkpoint: avançar pelo que realmente foi visto
        # se não viu nada, NÃO avance (isso evita “pular” histórico por acidente)
        if max_seen_updated_dt > checkpoint_dt:
            update_checkpoint(conn, max_seen_updated_dt)

    if stop_reason == "max_runtime":
        print(f"PARTIAL: upsert {orders_count} orders, {items_count} items. checkpoint={iso_z(max_seen_updated_dt)} (MAX_RUNTIME_MIN={MAX_RUNTIME_MIN})")
        return

    print(f"OK: upsert {orders_count} orders, {items_count} items. checkpoint={iso_z(max_seen_updated_dt)}")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            if LOCK_FILE.exists():
                LOCK_FILE.unlink()
        except Exception:
            pass