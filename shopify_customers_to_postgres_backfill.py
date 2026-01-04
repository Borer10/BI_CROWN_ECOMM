{\rtf1\ansi\ansicpg1252\cocoartf2867
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 import os, sys, time\
import requests\
from dotenv import load_dotenv\
from sqlalchemy import create_engine, text\
from datetime import datetime, timezone, timedelta\
from pathlib import Path\
\
load_dotenv()\
\
# =========================\
# LOCK FILE (anti concorr\'eancia)\
# =========================\
LOCK_FILE = Path("shopify_customers_backfill.lock")\
\
if LOCK_FILE.exists():\
    print("\uc0\u9940  Customers backfill j\'e1 est\'e1 rodando. Saindo para evitar execu\'e7\'e3o dupla.")\
    sys.exit(0)\
\
try:\
    LOCK_FILE.touch()\
except Exception as e:\
    print(f"Erro ao criar lock file: \{e\}")\
    sys.exit(1)\
\
SHOP = os.getenv("SHOPIFY_SHOP")\
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")\
VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")\
DB_URL = os.getenv("POSTGRES_URL")\
\
# Limite \'93amig\'e1vel\'94 (minutos) pra n\'e3o estourar Actions; 0 = sem limite\
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))\
START_TIME = time.time()\
\
if not all([SHOP, TOKEN, DB_URL]):\
    raise SystemExit("Faltou SHOPIFY_SHOP / SHOPIFY_ADMIN_TOKEN / POSTGRES_URL no .env")\
\
URL = f"https://\{SHOP\}/admin/api/\{VERSION\}/graphql.json"\
HEADERS = \{"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"\}\
\
# =========================\
# STATE (checkpoint) no Postgres\
# =========================\
PIPELINE_NAME = "shopify_customers_backfill"\
DEFAULT_START = "2020-01-01T00:00:00Z"   # pode ajustar depois se quiser\
SAFETY_WINDOW = timedelta(hours=2)      # reprocessa 2h para garantir consist\'eancia\
\
engine = create_engine(DB_URL, pool_pre_ping=True)\
\
STATE_TABLE_DDL = text("""\
create table if not exists public.etl_state (\
  pipeline text primary key,\
  checkpoint timestamptz not null,\
  updated_at timestamptz not null default now()\
);\
""")\
\
GET_STATE = text("""\
select checkpoint\
from public.etl_state\
where pipeline = :pipeline;\
""")\
\
UPSERT_STATE = text("""\
insert into public.etl_state (pipeline, checkpoint, updated_at)\
values (:pipeline, :checkpoint, now())\
on conflict (pipeline) do update\
set checkpoint = excluded.checkpoint,\
    updated_at = now();\
""")\
\
def iso_z(dt: datetime) -> str:\
    if dt.tzinfo is None:\
        dt = dt.replace(tzinfo=timezone.utc)\
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")\
\
def should_stop_soon() -> bool:\
    if MAX_RUNTIME_MIN <= 0:\
        return False\
    return ((time.time() - START_TIME) / 60.0) >= MAX_RUNTIME_MIN\
\
# =========================\
# SHOPIFY GRAPHQL\
# =========================\
QUERY = """\
query Customers($first: Int!, $after: String, $query: String) \{\
  customers(first: $first, after: $after, query: $query, sortKey: UPDATED_AT, reverse: false) \{\
    pageInfo \{ hasNextPage endCursor \}\
    edges \{\
      node \{\
        id\
        email\
        phone\
        firstName\
        lastName\
        displayName\
        state\
        verifiedEmail\
        acceptsMarketing\
        createdAt\
        updatedAt\
        defaultAddress \{\
          id\
        \}\
        addresses(first: 250) \{\
          edges \{\
            node \{\
              id\
              firstName\
              lastName\
              company\
              phone\
              address1\
              address2\
              city\
              province\
              provinceCode\
              zip\
              country\
              countryCodeV2\
              latitude\
              longitude\
            \}\
          \}\
        \}\
      \}\
    \}\
  \}\
\}\
"""\
\
def gql(variables):\
    r = requests.post(URL, headers=HEADERS, json=\{"query": QUERY, "variables": variables\}, timeout=60)\
    r.raise_for_status()\
    j = r.json()\
    if "errors" in j:\
        raise RuntimeError(j["errors"])\
    return j["data"]\
\
# =========================\
# UPSERTS\
# =========================\
UPSERT_CUSTOMER = text("""\
insert into public.customers (\
  customer_id, email, phone, first_name, last_name, display_name, state,\
  verified_email, accepts_marketing, created_at, updated_at, default_address_id, updated_db_at\
) values (\
  :customer_id, :email, :phone, :first_name, :last_name, :display_name, :state,\
  :verified_email, :accepts_marketing, :created_at, :updated_at, :default_address_id, now()\
)\
on conflict (customer_id) do update set\
  email = excluded.email,\
  phone = excluded.phone,\
  first_name = excluded.first_name,\
  last_name = excluded.last_name,\
  display_name = excluded.display_name,\
  state = excluded.state,\
  verified_email = excluded.verified_email,\
  accepts_marketing = excluded.accepts_marketing,\
  created_at = excluded.created_at,\
  updated_at = excluded.updated_at,\
  default_address_id = excluded.default_address_id,\
  updated_db_at = now();\
""")\
\
UPSERT_ADDRESS = text("""\
insert into public.customer_addresses (\
  address_id, customer_id, is_default,\
  first_name, last_name, company, phone,\
  address1, address2, city, province, province_code, zip,\
  country, country_code, latitude, longitude, updated_db_at\
) values (\
  :address_id, :customer_id, :is_default,\
  :first_name, :last_name, :company, :phone,\
  :address1, :address2, :city, :province, :province_code, :zip,\
  :country, :country_code, :latitude, :longitude, now()\
)\
on conflict (address_id) do update set\
  customer_id = excluded.customer_id,\
  is_default = excluded.is_default,\
  first_name = excluded.first_name,\
  last_name = excluded.last_name,\
  company = excluded.company,\
  phone = excluded.phone,\
  address1 = excluded.address1,\
  address2 = excluded.address2,\
  city = excluded.city,\
  province = excluded.province,\
  province_code = excluded.province_code,\
  zip = excluded.zip,\
  country = excluded.country,\
  country_code = excluded.country_code,\
  latitude = excluded.latitude,\
  longitude = excluded.longitude,\
  updated_db_at = now();\
""")\
\
def main():\
    customers_count = 0\
    addresses_count = 0\
\
    # 1) carrega checkpoint\
    with engine.begin() as conn:\
        conn.execute(STATE_TABLE_DDL)\
        row = conn.execute(GET_STATE, \{"pipeline": PIPELINE_NAME\}).fetchone()\
        if not row:\
            checkpoint_dt = datetime.fromisoformat(DEFAULT_START.replace("Z", "+00:00"))\
            conn.execute(UPSERT_STATE, \{"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt\})\
        else:\
            checkpoint_dt = row[0]\
\
    # safety window (reprocessar 2h)\
    effective_start = checkpoint_dt - SAFETY_WINDOW\
    query_filter = f"updated_at:>=\{iso_z(effective_start)\}"\
\
    after = None\
    max_seen_updated = checkpoint_dt\
\
    # 2) loop paginado\
    while True:\
        if should_stop_soon():\
            # n\'e3o avan\'e7a checkpoint al\'e9m do que de fato consolidamos\
            with engine.begin() as conn:\
                conn.execute(UPSERT_STATE, \{"pipeline": PIPELINE_NAME, "checkpoint": max_seen_updated\})\
            print(f"PARTIAL: runtime_limit reached. customers=\{customers_count\} addresses=\{addresses_count\} checkpoint=\{iso_z(max_seen_updated)\}")\
            return\
\
        data = gql(\{"first": 100, "after": after, "query": query_filter\})\
        edges = data["customers"]["edges"]\
\
        if not edges:\
            break\
\
        with engine.begin() as conn:\
            for e in edges:\
                c = e["node"]\
                cid = c["id"]\
                updated_at = datetime.fromisoformat(c["updatedAt"].replace("Z", "+00:00"))\
\
                if updated_at > max_seen_updated:\
                    max_seen_updated = updated_at\
\
                default_addr = (c.get("defaultAddress") or \{\}).get("id")\
\
                conn.execute(UPSERT_CUSTOMER, \{\
                    "customer_id": cid,\
                    "email": c.get("email"),\
                    "phone": c.get("phone"),\
                    "first_name": c.get("firstName"),\
                    "last_name": c.get("lastName"),\
                    "display_name": c.get("displayName"),\
                    "state": c.get("state"),\
                    "verified_email": c.get("verifiedEmail"),\
                    "accepts_marketing": c.get("acceptsMarketing"),\
                    "created_at": c.get("createdAt"),\
                    "updated_at": c.get("updatedAt"),\
                    "default_address_id": default_addr,\
                \})\
                customers_count += 1\
\
                for a in ((c.get("addresses") or \{\}).get("edges") or []):\
                    n = a["node"]\
                    aid = n["id"]\
                    conn.execute(UPSERT_ADDRESS, \{\
                        "address_id": aid,\
                        "customer_id": cid,\
                        "is_default": (default_addr == aid),\
                        "first_name": n.get("firstName"),\
                        "last_name": n.get("lastName"),\
                        "company": n.get("company"),\
                        "phone": n.get("phone"),\
                        "address1": n.get("address1"),\
                        "address2": n.get("address2"),\
                        "city": n.get("city"),\
                        "province": n.get("province"),\
                        "province_code": n.get("provinceCode"),\
                        "zip": n.get("zip"),\
                        "country": n.get("country"),\
                        "country_code": n.get("countryCodeV2"),\
                        "latitude": n.get("latitude"),\
                        "longitude": n.get("longitude"),\
                    \})\
                    addresses_count += 1\
\
        page = data["customers"]["pageInfo"]\
        if not page["hasNextPage"]:\
            break\
        after = page["endCursor"]\
        time.sleep(0.35)\
\
    # 3) grava checkpoint final\
    with engine.begin() as conn:\
        conn.execute(UPSERT_STATE, \{"pipeline": PIPELINE_NAME, "checkpoint": max_seen_updated\})\
\
    print(f"OK: upsert \{customers_count\} customers, \{addresses_count\} addresses. checkpoint=\{iso_z(max_seen_updated)\}")\
\
if __name__ == "__main__":\
    try:\
        main()\
    finally:\
        if LOCK_FILE.exists():\
            LOCK_FILE.unlink()}