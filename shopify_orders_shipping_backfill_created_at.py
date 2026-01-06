#!/usr/bin/env python3
import os, time, sys
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime, timezone
from pathlib import Path

load_dotenv()

# =========================
# LOCK FILE (anti concorrência)
# =========================
LOCK_FILE = Path("shopify_orders_shipping_backfill.lock")

if LOCK_FILE.exists():
    print("⛔ Backfill já está rodando. Saindo para evitar execução dupla.")
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

MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "18"))  # backfill costuma ter limite
START_TIME = time.time()

BACKFILL_START_ISO = os.getenv("BACKFILL_START_ISO", "2025-12-01T00:00:00Z").strip()

if not all([SHOP, TOKEN, DB_URL]):
    raise SystemExit("Faltou SHOPIFY_SHOP / SHOPIFY_ADMIN_TOKEN / POSTGRES_URL no .env")

URL = f"https://{SHOP}/admin/api/{VERSION}/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

engine = create_engine(DB_URL, pool_pre_ping=True)

def should_stop_soon():
    if MAX_RUNTIME_MIN <= 0:
        return False
    elapsed_min = (time.time() - START_TIME) / 60.0
    return elapsed_min >= MAX_RUNTIME_MIN

def gql(variables):
    r = requests.post(URL, headers=HEADERS, json={"query": QUERY, "variables": variables}, timeout=60)
    r.raise_for_status()
    j = r.json()
    if "errors" in j:
        raise RuntimeError(j["errors"])
    return j["data"]

def parse_iso(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

def norm_float(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None

# =========================
# Shopify Query (por CREATED_AT)
# =========================
QUERY = """
query Orders($first: Int!, $after: String, $query: String) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: false) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        createdAt
        updatedAt
        shippingAddress {
          city
          province
          country
          zip
          latitude
          longitude
        }
      }
    }
  }
}
"""

# =========================
# UPSERT (somente campos de shipping)
# idempotente por order_id
# =========================
UPSERT_SHIPPING = text("""
update public.orders
set
  shipping_city = :shipping_city,
  shipping_province = :shipping_province,
  shipping_country = :shipping_country,
  shipping_zip = :shipping_zip,
  shipping_latitude = :shipping_latitude,
  shipping_longitude = :shipping_longitude,
  updated_db_at = now()
where order_id = :order_id;
""")

# Observação:
# Aqui eu uso UPDATE (não INSERT) porque:
# - você já tem os pedidos antigos do backfill
# - é mais seguro: não cria linhas novas se por algum motivo vier pedido fora do seu dataset
#
# Se você preferir UPSERT completo, dá pra trocar por INSERT...ON CONFLICT, mas não é necessário.

def main():
    start_dt = parse_iso(BACKFILL_START_ISO)
    start_str = start_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    query_filter = f"created_at:>={start_str}"

    print(f"BACKFILL SHIPPING (created_at) desde {start_str}")
    print(f"Query='{query_filter}'")

    after = None
    updated_orders = 0
    scanned_orders = 0

    with engine.begin() as conn:
        while True:
            if should_stop_soon():
                print(f"PARTIAL: atualizados {updated_orders} pedidos (escaneados {scanned_orders}). MAX_RUNTIME_MIN={MAX_RUNTIME_MIN}")
                return

            data = gql({"first": 100, "after": after, "query": query_filter})
            edges = data["orders"]["edges"]
            page = data["orders"]["pageInfo"]

            if not edges:
                if not page["hasNextPage"]:
                    break
                after = page["endCursor"]
                time.sleep(0.35)
                continue

            for e in edges:
                o = e["node"]
                ship = o.get("shippingAddress") or {}

                scanned_orders += 1

                # Só tenta atualizar se vier algum dado de shipping
                has_any = any([
                    ship.get("city"), ship.get("province"), ship.get("country"), ship.get("zip"),
                    ship.get("latitude") is not None, ship.get("longitude") is not None
                ])
                if not has_any:
                    continue

                res = conn.execute(UPSERT_SHIPPING, {
                    "order_id": o["id"],
                    "shipping_city": ship.get("city"),
                    "shipping_province": ship.get("province"),
                    "shipping_country": ship.get("country"),
                    "shipping_zip": ship.get("zip"),
                    "shipping_latitude": norm_float(ship.get("latitude")),
                    "shipping_longitude": norm_float(ship.get("longitude")),
                })

                # rowcount = 1 se encontrou order_id na tabela e atualizou
                if res.rowcount and res.rowcount > 0:
                    updated_orders += 1

            if not page["hasNextPage"]:
                break
            after = page["endCursor"]
            time.sleep(0.35)

    print(f"OK: shipping atualizado em {updated_orders} pedidos (escaneados {scanned_orders}) desde {start_str}")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            if LOCK_FILE.exists():
                LOCK_FILE.unlink()
        except Exception:
            pass