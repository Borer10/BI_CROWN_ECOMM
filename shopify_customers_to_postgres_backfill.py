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
LOCK_FILE = Path("shopify_customers_backfill.lock")

if LOCK_FILE.exists():
    print("⛔ Customers backfill já está rodando. Saindo para evitar execução dupla.")
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
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "18"))  # sugiro 18 num job de 20 min
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
    elapsed_min = (time.time() - START_TIME) / 60.0
    return elapsed_min >= MAX_RUNTIME_MIN

def gql(variables):
    r = requests.post(URL, headers=HEADERS, json={"query": QUERY, "variables": variables}, timeout=60)
    r.raise_for_status()
    j = r.json()
    if "errors" in j:
        raise RuntimeError(j["errors"])
    return j["data"]

def to_bool_accepts_marketing(email_marketing_consent: str | None):
    # emailMarketingConsent é ENUM (ex: SUBSCRIBED/UNSUBSCRIBED/NOT_SUBSCRIBED/UNKNOWN etc)
    if not email_marketing_consent:
        return None
    return email_marketing_consent.upper() == "SUBSCRIBED"

# =========================
# SHOPIFY QUERY (Admin GraphQL 2025-01 compatível)
# - emailMarketingConsent e smsMarketingConsent são ENUMs (não objetos)
# - para paginação de endereços use addressesV2 (connection)
# =========================
QUERY = """
query Customers($first: Int!, $after: String) {
  customers(first: $first, after: $after) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        legacyResourceId
        email
        firstName
        lastName
        displayName
        phone
        verifiedEmail
        state
        tags
        createdAt
        updatedAt

        defaultAddress {
          id
          firstName
          lastName
          address1
          address2
          city
          province
          country
          zip
          phone
        }
      }
    }
  }
}
"""
# =========================
# UPSERTS
# =========================
CUSTOMERS_TABLE_DDL = text("""
create table if not exists public.customers (
  customer_id text primary key,
  email text,
  phone text,
  first_name text,
  last_name text,
  display_name text,
  state text,
  verified_email boolean,
  accepts_marketing boolean,
  email_marketing_consent text,
  sms_marketing_consent text,
  created_at timestamptz,
  updated_at timestamptz,
  default_address_id text,
  updated_db_at timestamptz not null default now()
);
""")

ADDRESSES_TABLE_DDL = text("""
create table if not exists public.customer_addresses (
  address_id text primary key,
  customer_id text not null,
  is_default boolean,
  first_name text,
  last_name text,
  company text,
  phone text,
  address1 text,
  address2 text,
  city text,
  province text,
  province_code text,
  zip text,
  country text,
  country_code text,
  latitude double precision,
  longitude double precision,
  updated_db_at timestamptz not null default now()
);
""")

UPSERT_CUSTOMER = text("""
insert into public.customers (
  customer_id, email, phone, first_name, last_name, display_name, state,
  verified_email, accepts_marketing, email_marketing_consent, sms_marketing_consent,
  created_at, updated_at, default_address_id, updated_db_at
) values (
  :customer_id, :email, :phone, :first_name, :last_name, :display_name, :state,
  :verified_email, :accepts_marketing, :email_marketing_consent, :sms_marketing_consent,
  :created_at, :updated_at, :default_address_id, now()
)
on conflict (customer_id) do update set
  email = excluded.email,
  phone = excluded.phone,
  first_name = excluded.first_name,
  last_name = excluded.last_name,
  display_name = excluded.display_name,
  state = excluded.state,
  verified_email = excluded.verified_email,
  accepts_marketing = excluded.accepts_marketing,
  email_marketing_consent = excluded.email_marketing_consent,
  sms_marketing_consent = excluded.sms_marketing_consent,
  created_at = excluded.created_at,
  updated_at = excluded.updated_at,
  default_address_id = excluded.default_address_id,
  updated_db_at = now();
""")

UPSERT_ADDRESS = text("""
insert into public.customer_addresses (
  address_id, customer_id, is_default,
  first_name, last_name, company, phone,
  address1, address2, city, province, province_code,
  zip, country, country_code,
  latitude, longitude,
  updated_db_at
) values (
  :address_id, :customer_id, :is_default,
  :first_name, :last_name, :company, :phone,
  :address1, :address2, :city, :province, :province_code,
  :zip, :country, :country_code,
  :latitude, :longitude,
  now()
)
on conflict (address_id) do update set
  customer_id = excluded.customer_id,
  is_default = excluded.is_default,
  first_name = excluded.first_name,
  last_name = excluded.last_name,
  company = excluded.company,
  phone = excluded.phone,
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
    after = None

    max_seen_updated_at = None

    with engine.connect() as conn:
        # garantir tabelas e checkpoint
        with conn.begin():
            conn.execute(STATE_TABLE_DDL)
            conn.execute(CUSTOMERS_TABLE_DDL)
            conn.execute(ADDRESSES_TABLE_DDL)

            row = conn.execute(GET_STATE, {"pipeline": PIPELINE_NAME}).fetchone()
            if not row:
                checkpoint_dt = datetime.fromisoformat(DEFAULT_START.replace("Z", "+00:00"))
                conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt})
            else:
                checkpoint_dt = row[0]

        effective_start = checkpoint_dt - SAFETY_WINDOW
        query_filter = f"updated_at:>={iso_z(effective_start)}"
        print(f"▶️ Pipeline={PIPELINE_NAME} checkpoint={checkpoint_dt} effective_start={effective_start} query='{query_filter}'")

        while True:
            if should_stop_soon():
                print("⏱️ Cheguei perto do limite de runtime. Salvando checkpoint e saindo...")
                break

            data = gql({"first": 100, "after": after, "query": query_filter})
            payload = data["customers"]
            edges = payload["edges"]

            if not edges:
                print("✅ Sem mais customers nesse range.")
                break

            with conn.begin():
                for e in edges:
                    c = e["node"]
                    customer_id = c["id"]
                    updated_at = datetime.fromisoformat(c["updatedAt"].replace("Z", "+00:00"))

                    if (max_seen_updated_at is None) or (updated_at > max_seen_updated_at):
                        max_seen_updated_at = updated_at

                    default_address_id = c["defaultAddress"]["id"] if c.get("defaultAddress") else None

                    email_marketing_consent = (
                        c.get("emailMarketingConsent", {}) or {}
                    ).get("state")
                    
                    sms_marketing_consent = (
                        c.get("smsMarketingConsent", {}) or {}
                    ).get("state")

                    conn.execute(UPSERT_CUSTOMER, {
                        "customer_id": customer_id,
                        "email": c.get("email"),
                        "phone": c.get("phone"),
                        "first_name": c.get("firstName"),
                        "last_name": c.get("lastName"),
                        "display_name": c.get("displayName"),
                        "state": c.get("state"),
                        "verified_email": c.get("verifiedEmail"),
                        "accepts_marketing": to_bool_accepts_marketing(email_marketing_consent),
                        "email_marketing_consent": email_marketing_consent,
                        "sms_marketing_consent": sms_marketing_consent,
                        "created_at": datetime.fromisoformat(c["createdAt"].replace("Z", "+00:00")) if c.get("createdAt") else None,
                        "updated_at": updated_at,
                        "default_address_id": default_address_id
                    })
                    customers_count += 1

                    # endereços (connection)
                    addr_edges = (((c.get("addressesV2") or {}).get("edges")) or [])
                    for ae in addr_edges:
                        a = ae["node"]
                        address_id = a["id"]
                        conn.execute(UPSERT_ADDRESS, {
                            "address_id": address_id,
                            "customer_id": customer_id,
                            "is_default": (default_address_id == address_id),
                            "first_name": a.get("firstName"),
                            "last_name": a.get("lastName"),
                            "company": a.get("company"),
                            "phone": a.get("phone"),
                            "address1": a.get("address1"),
                            "address2": a.get("address2"),
                            "city": a.get("city"),
                            "province": a.get("province"),
                            "province_code": a.get("provinceCode"),
                            "zip": a.get("zip"),
                            "country": a.get("country"),
                            "country_code": a.get("countryCodeV2"),
                            "latitude": a.get("latitude"),
                            "longitude": a.get("longitude"),
                        })
                        addresses_count += 1

            page_info = payload["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            after = page_info["endCursor"]

        # atualizar checkpoint
        if max_seen_updated_at is not None:
            with conn.begin():
                conn.execute(UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": max_seen_updated_at})
            print(f"✅ Checkpoint atualizado para {max_seen_updated_at}")
        else:
            print("ℹ️ Nenhum customer processado nessa execução; checkpoint mantido.")

    print(f"✅ DONE customers={customers_count} addresses={addresses_count}")

if __name__ == "__main__":
    try:
        main()
    finally:
        # sempre remove o lock
        try:
            LOCK_FILE.unlink(missing_ok=True)
        except Exception:
            pass
