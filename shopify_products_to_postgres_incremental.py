#!/usr/bin/env python3
import os
import time
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from sqlalchemy import create_engine, text


# =========================
# LOCK FILE (anti concorrência)
# =========================
LOCK_FILE = Path("shopify_products_incremental.lock")

if LOCK_FILE.exists():
    print("⛔ Products ETL já está rodando. Saindo para evitar execução dupla.")
    sys.exit(0)

try:
    LOCK_FILE.touch()
except Exception as e:
    print(f"Erro ao criar lock file: {e}")
    sys.exit(1)


# =========================
# ENV
# =========================
SHOP = os.getenv("SHOPIFY_SHOP")  # ex: canetas-crown-n1.myshopify.com
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")
DB_URL = os.getenv("POSTGRES_URL")

MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))  # 0 = sem limite
START_TIME = time.time()

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "shopify_products")

# O "DEFAULT_START" é só para o primeiro run, quando ainda não existe checkpoint.
# Como você quer incremental diário, pode deixar bem antigo (ex: 2020) sem problema,
# porque o checkpoint vai assumir depois.
DEFAULT_START = os.getenv("DEFAULT_START", "2020-01-01T00:00:00Z")

# Safety window para pegar updates atrasados / race conditions
SAFETY_WINDOW = timedelta(hours=4)

if not all([SHOP, TOKEN, DB_URL]):
    raise SystemExit("Faltou SHOPIFY_SHOP / SHOPIFY_ADMIN_TOKEN / POSTGRES_URL nas env vars")

URL = f"https://{SHOP}/admin/api/{VERSION}/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}


# =========================
# DB / STATE (etl_state)
# =========================
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


# =========================
# TARGET TABLE (assume que você já criou com DDL)
# public.dim_product_variants
# =========================
UPSERT_DIM_VARIANT = text("""
insert into public.dim_product_variants (
  variant_id,
  product_id,
  sku,
  barcode,
  variant_title,
  option1,
  option2,
  option3,
  price,
  compare_at_price,
  inventory_item_id,
  product_title,
  vendor,
  product_type,
  product_status,
  product_created_at,
  product_updated_at,
  variant_created_at,
  variant_updated_at,
  updated_db_at
) values (
  :variant_id,
  :product_id,
  :sku,
  :barcode,
  :variant_title,
  :option1,
  :option2,
  :option3,
  :price,
  :compare_at_price,
  :inventory_item_id,
  :product_title,
  :vendor,