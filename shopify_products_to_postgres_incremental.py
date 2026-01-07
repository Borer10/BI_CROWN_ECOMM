#!/usr/bin/env python3
import os
import time
from datetime import datetime, timezone, timedelta

import requests
from sqlalchemy import create_engine, text

# =========================
# ENV
# =========================
SHOP = os.getenv("SHOPIFY_SHOP")  # ex: canetas-crown-n1.myshopify.com
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")
POSTGRES_URL = os.getenv("POSTGRES_URL")

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "shopify_products")
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "25"))  # diário
DEFAULT_START = os.getenv("DEFAULT_START", "2025-01-01T00:00:00Z")
SAFETY_WINDOW = timedelta(hours=2)

if not all([SHOP, TOKEN, POSTGRES_URL]):
    raise SystemExit("Faltou SHOPIFY_SHOP / SHOPIFY_ADMIN_TOKEN / POSTGRES_URL")

URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

START_TS = time.time()

# =========================
# DB
# =========================
engine = create_engine(POSTGRES_URL, pool_pre_ping=True)

DDL_ETL_STATE = text("""
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

DDL_PRODUCT_VARIANTS = text("""
create table if not exists public.product_variants (
  variant_id text primary key,
  product_id text,
  product_title text,
  product_handle text,
  product_vendor text,
  product_type text,
  product_status text,

  variant_title text,
  sku text,
  barcode text,

  price numeric,
  compare_at_price numeric,
  inventory_quantity integer,
  taxable boolean,

  variant_created_at timestamptz,
  variant_updated_at timestamptz,

  selected_options jsonb,
  updated_db_at timestamptz not null default now()
);
""")

UPSERT_VARIANT = text("""
insert into public.product_variants (
  variant_id, product_id, product_title, product_handle, product_vendor, product_type, product_status,
  variant_title, sku, barcode,
  price, compare_at_price, inventory_quantity, taxable,
  variant_created_at, variant_updated_at,
  selected_options, updated_db_at
) values (
  :variant_id, :product_id, :product_title, :product_handle, :product_vendor, :product_type, :product_status,
  :variant_title, :sku, :barcode,
  :price, :compare_at_price, :inventory_quantity, :taxable,
  :variant_created_at, :variant_updated_at,
  :selected_options, now()
)
on conflict (variant_id) do update set
  product_id = excluded.product_id,
  product_title = excluded.product_title,
  product_handle = excluded.product_handle,
  product_vendor = excluded.product_vendor,
  product_type = excluded.product_type,
  product_status = excluded.product_status,
  variant_title = excluded.variant_title,
  sku = excluded.sku,
  barcode = excluded.barcode,
  price = excluded.price,
  compare_at_price = excluded.compare_at_price,
  inventory_quantity = excluded.inventory_quantity,
  taxable = excluded.taxable,
  variant_created_at = excluded.variant_created_at,
  variant_updated_at = excluded.variant_updated_at,
  selected_options = excluded.selected_options,
  updated_db_at = now();
""")

# tenta dar refresh se existir
REFRESH_MV = text("refresh materialized view analytics.mv_dim_product_variants;")

# =========================
# Shopify GraphQL
# =========================
# Importante: incremental por UPDATED_AT do PRODUCT (não do variant)
QUERY = """
query Products($first: Int!, $after: String, $query: String) {
  products(first: $first, after: $after, query: $query, sortKey: UPDATED_AT, reverse: false) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        title
        handle
        vendor
        productType
        status
        updatedAt
        createdAt

        variants(first: 250) {
          nodes {
            id
            title
            sku
            barcode
            price
            compareAtPrice
            inventoryQuantity
            taxable
            createdAt
            updatedAt
            selectedOptions { name value }
          }
        }
      }
    }
  }
}
""".strip()

def gql(variables: dict):
    r = requests.post(URL, headers=HEADERS, json={"query": QUERY, "variables": variables}, timeout=60)
    r.raise_for_status()
    j = r.json()
    if "errors" in j and j["errors"]:
        raise RuntimeError(j["errors"])
    return j["data"]

def parse_iso(s: str):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

def should_stop_soon():
    if MAX_RUNTIME_MIN <= 0:
        return False
    return ((time.time() - START_TS) / 60.0) >= MAX_RUNTIME_MIN

def ensure_checkpoint(conn) -> datetime:
    conn.execute(DDL_ETL_STATE)
    row = conn.execute(GET_STATE, {"pipeline": PIPELINE_NAME}).fetchone()
    if row and row[0]:
        return row[0]
    start_dt = parse_iso(DEFAULT_START)
    conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": start_dt})
    return start_dt

def update_checkpoint(conn, dt: datetime):
    conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": dt})

def build_query_filter(checkpoint_dt: datetime):
    # filtro aplicado em PRODUCTS.updated_at
    safe_start = checkpoint_dt - SAFETY_WINDOW
    default_start_dt = parse_iso(DEFAULT_START)
    if safe_start < default_start_dt:
        safe_start = default_start_dt
    return f"updated_at:>={iso_z(safe_start)}", safe_start

def to_num(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None

def to_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None

def main():
    variants_count = 0
    products_count = 0

    with engine.begin() as conn:
        conn.execute(DDL_PRODUCT_VARIANTS)
        checkpoint_dt = ensure_checkpoint(conn)

    query_filter, safe_start_dt = build_query_filter(checkpoint_dt)
    print(f"PIPELINE={PIPELINE_NAME} checkpoint={iso_z(checkpoint_dt)} safety_start={iso_z(safe_start_dt)} query='{query_filter}'")

    after = None
    # ✅ checkpoint vai avançar pelo product.updatedAt (ordenável)
    max_seen_product_updated_dt = checkpoint_dt

    with engine.begin() as conn:
        while True:
            if should_stop_soon():
                break

            data = gql({"first": 80, "after": after, "query": query_filter})
            edges = data["products"]["edges"]
            page = data["products"]["pageInfo"]

            if not edges:
                if not page["hasNextPage"]:
                    break
                after = page["endCursor"]
                time.sleep(0.35)
                continue

            for e in edges:
                p = e["node"]
                products_count += 1

                p_updated = parse_iso(p.get("updatedAt"))
                if p_updated and p_updated > max_seen_product_updated_dt:
                    max_seen_product_updated_dt = p_updated

                variants = ((p.get("variants") or {}).get("nodes")) or []
                for v in variants:
                    v_updated = parse_iso(v.get("updatedAt"))

                    conn.execute(UPSERT_VARIANT, {
                        "variant_id": v.get("id"),
                        "product_id": p.get("id"),
                        "product_title": p.get("title"),
                        "product_handle": p.get("handle"),
                        "product_vendor": p.get("vendor"),
                        "product_type": p.get("productType"),
                        "product_status": p.get("status"),

                        "variant_title": v.get("title"),
                        "sku": v.get("sku"),
                        "barcode": v.get("barcode"),

                        "price": to_num(v.get("price")),
                        "compare_at_price": to_num(v.get("compareAtPrice")),
                        "inventory_quantity": to_int(v.get("inventoryQuantity")),
                        "taxable": v.get("taxable"),

                        "variant_created_at": parse_iso(v.get("createdAt")),
                        "variant_updated_at": v_updated,

                        "selected_options": v.get("selectedOptions"),
                    })
                    variants_count += 1

            if not page["hasNextPage"]:
                break

            after = page["endCursor"]
            time.sleep(0.35)

        # ✅ checkpoint só avança se viu products mais novos
        if max_seen_product_updated_dt > checkpoint_dt:
            update_checkpoint(conn, max_seen_product_updated_dt)

        # tenta refresh da MV (se ela existir)
        try:
            conn.execute(REFRESH_MV)
            refreshed = True
        except Exception:
            refreshed = False

    # logs finais
    if variants_count == 0:
        print(
            f"OK: upsert 0 variants (products_scanned={products_count}). "
            f"checkpoint={iso_z(max_seen_product_updated_dt)} (nada novo) refreshed_mv={refreshed}"
        )
        return

    if should_stop_soon():
        print(
            f"PARTIAL: upsert {variants_count} variants (products_scanned={products_count}). "
            f"checkpoint={iso_z(max_seen_product_updated_dt)} refreshed_mv={refreshed}"
        )
        return

    print(
        f"OK: upsert {variants_count} variants (products_scanned={products_count}). "
        f"checkpoint={iso_z(max_seen_product_updated_dt)} refreshed_mv={refreshed}"
    )

if __name__ == "__main__":
    main()