import os, sys, time
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from datetime import datetime, timezone, timedelta
from pathlib import Path

load_dotenv()

# =========================
# LOCK FILE (anti concorrência)
# =========================
LOCK_FILE = Path("shopify_customers_backfill.lock")

if LOCK_FILE.exists():
    print("⛔ Backfill já está rodando. Saindo para evitar execução dupla.")
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
PIPELINE_NAME = "shopify_customers"
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

def should_stop_soon():
    if MAX_RUNTIME_MIN <= 0:
        return False
    elapsed_min = (time.time() - START_TIME) / 60.0
    return elapsed_min >= MAX_RUNTIME_MIN

# =========================
# SHOPIFY QUERY (corrigida)
# =========================
QUERY = """
query Customers($first: Int!, $after: String, $query: String) {
  customers(first: $first, after: $after, query: $query, sortKey: UPDATED_AT, reverse: false) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        email
        firstName
        lastName
        phone
        note
        tags
        verifiedEmail
        createdAt
        updatedAt

        # Substitui acceptsMarketing (que pode não existir)
        emailMarketingConsent { marketingState optInLevel consentUpdatedAt }
        smsMarketingConsent { marketingState optInLevel consentUpdatedAt }

        defaultAddress { id }

        # No seu schema, addresses está vindo como LISTA de MailingAddress (sem edges)
        addresses {
          id
          firstName
          lastName
          phone
          company
          address1
          address2
          city
          province
          provinceCode
          zip
          country
          countryCodeV2
          latitude
          longitude
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

# =========================
# UPSERTS (assumindo que as tabelas já existem)
# =========================
UPSERT_CUSTOMER = text("""
insert into public.customers (
  customer_id,
  email,
  first_name,
  last_name,
  phone,
  verified_email,
  accepts_marketing,
  tags,
  note,
  created_at,
  updated_at,
  default_address_id,
  updated_db_at
) values (
  :customer_id,
  :email,
  :first_name,
  :last_name,
  :phone,
  :verified_email,
  :accepts_marketing,
  :tags,
  :note,
  :created_at,
  :updated_at,
  :default_address_id,
  now()
)
on conflict (customer_id) do update set
  email = excluded.email,
  first_name = excluded.first_name,
  last_name = excluded.last_name,
  phone = excluded.phone,
  verified_email = excluded.verified_email,
  accepts_marketing = excluded.accepts_marketing,
  tags = excluded.tags,
  note = excluded.note,
  created_at = excluded.created_at,
  updated_at = excluded.updated_at,
  default_address_id = excluded.default_address_id,
  updated_db_at = now();
""")

UPSERT_ADDRESS = text("""
insert into public.customer_addresses (
  address_id,
  customer_id,
  first_name,
  last_name,
  phone,
  company,
  address1,
  address2,
  city,
  province,
  province_code,
  zip,
  country,
  country_code,
  latitude,
  longitude,
  updated_db_at
) values (
  :address_id,
  :customer_id,
  :first_name,
  :last_name,
  :phone,
  :company,
  :address1,
  :address2,
  :city,
  :province,
  :province_code,
  :zip,
  :country,
  :country_code,
  :latitude,
  :longitude,
  now()
)
on conflict (address_id) do update set
  customer_id = excluded.customer_id,
  first_name = excluded.first_name,
  last_name = excluded.last_name,
  phone = excluded.phone,
  company = excluded.company,
  address1 = excluded.address1,
  address2 = excluded.address2,
  city = excluded.city,
  province = excluded.province,
  province_code = excluded.province_code,
  zip = excluded.zip,
  country = excluded.country,
  country_code = excluded.country_code,
  latitude = excluded.latitude,
  longitude = excluded.longitude,
  updated_db_at = now();
""")

def main():
    customers_count = 0
    addresses_count = 0

    with engine.connect() as conn:
        # garante tabela de state e pega checkpoint (tudo dentro de transação)
        with conn.begin():
            conn.execute(STATE_TABLE_DDL)
            row = conn.execute(GET_STATE, {"pipeline": PIPELINE_NAME}).fetchone()
            if not row:
                checkpoint_dt = datetime.fromisoformat(DEFAULT_START.replace("Z", "+00:00"))
                conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt})
            else:
                checkpoint_dt = row[0]

        effective_start_dt = (checkpoint_dt - SAFETY_WINDOW).astimezone(timezone.utc)
        query_filter = f"updated_at:>={iso_z(effective_start_dt)}"

        after = None
        max_seen_updated = checkpoint_dt

        while True:
            if should_stop_soon():
                # grava o melhor checkpoint possível e sai “bonito”
                with conn.begin():
                    conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": max_seen_updated})
                print(f"PARTIAL: customers={customers_count}, addresses={addresses_count}, checkpoint={iso_z(max_seen_updated)}")
                return

            data = gql({"first": 100, "after": after, "query": query_filter})
            edges = data["customers"]["edges"]

            if not edges and not data["customers"]["pageInfo"]["hasNextPage"]:
                break

            # upserts em lotes por página
            with conn.begin():
                for e in edges:
                    c = e["node"]

                    # atualiza watermark
                    if c["updatedAt"] > iso_z(max_seen_updated):
                        max_seen_updated = datetime.fromisoformat(c["updatedAt"].replace("Z", "+00:00"))

                    default_addr = (c.get("defaultAddress") or {}).get("id")

                    # aceita marketing (derivado do marketingState)
                    email_state = (c.get("emailMarketingConsent") or {}).get("marketingState")
                    accepts_marketing = email_state in ("SUBSCRIBED", "CONFIRMED")

                    conn.execute(UPSERT_CUSTOMER, {
                        "customer_id": c["id"],
                        "email": c.get("email"),
                        "first_name": c.get("firstName"),
                        "last_name": c.get("lastName"),
                        "phone": c.get("phone"),
                        "verified_email": c.get("verifiedEmail"),
                        "accepts_marketing": accepts_marketing,
                        "tags": ",".join(c.get("tags") or []) if isinstance(c.get("tags"), list) else (c.get("tags") or ""),
                        "note": c.get("note"),
                        "created_at": c.get("createdAt"),
                        "updated_at": c.get("updatedAt"),
                        "default_address_id": default_addr,
                    })
                    customers_count += 1

                    # addresses: LISTA direta (sem edges/node)
                    for n in (c.get("addresses") or []):
                        aid = n.get("id")
                        if not aid:
                            continue

                        conn.execute(UPSERT_ADDRESS, {
                            "address_id": aid,
                            "customer_id": c["id"],
                            "first_name": n.get("firstName"),
                            "last_name": n.get("lastName"),
                            "phone": n.get("phone"),
                            "company": n.get("company"),
                            "address1": n.get("address1"),
                            "address2": n.get("address2"),
                            "city": n.get("city"),
                            "province": n.get("province"),
                            "province_code": n.get("provinceCode"),
                            "zip": n.get("zip"),
                            "country": n.get("country"),
                            "country_code": n.get("countryCodeV2"),
                            "latitude": n.get("latitude"),
                            "longitude": n.get("longitude"),
                        })
                        addresses_count += 1

                # avança checkpoint ao final da página (watermark)
                conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": max_seen_updated})

            page = data["customers"]["pageInfo"]
            if not page["hasNextPage"]:
                break
            after = page["endCursor"]
            time.sleep(0.35)

    print(f"OK: upsert {customers_count} customers, {addresses_count} addresses. checkpoint={iso_z(max_seen_updated)}")

if __name__ == "__main__":
    try:
        main()
    finally:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()