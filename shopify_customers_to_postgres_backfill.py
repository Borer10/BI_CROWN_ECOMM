#!/usr/bin/env python3
import os
import sys
import json
import time
import re
from datetime import datetime, timezone, timedelta

import requests
import psycopg2


# =========================
# Config
# =========================
SHOP = os.getenv("SHOPIFY_SHOP")  # ex: "canetas-crown-n1.myshopify.com"
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")

POSTGRES_URL = os.getenv("POSTGRES_URL")

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "shopify_customers")
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "18"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))

# Se você quiser forçar um "start" manual (ex: backfill do histórico inteiro), pode setar:
# EFFECTIVE_START_ISO="2024-01-01T00:00:00Z"
EFFECTIVE_START_ISO = os.getenv("EFFECTIVE_START_ISO", "").strip()

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"


# =========================
# GraphQL Query (Customer + Addresses)
# =========================
QUERY = """
query Customers($first: Int!, $after: String, $query: String) {
  customers(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        email
        firstName
        lastName
        displayName
        phone
        state
        verifiedEmail
        dataSaleOptOut
        createdAt
        updatedAt
        defaultAddress { id }

        addressesV2(first: 250) {
          nodes {
            id
            firstName
            lastName
            company
            address1
            address2
            city
            province
            provinceCode
            country
            countryCodeV2
            zip
            phone
            latitude
            longitude
          }
        }
      }
    }
  }
}
""".strip()


# =========================
# SQL
# =========================
SQL_CUSTOMERS_UPSERT = """
insert into public.customers (
  customer_id, email, phone, first_name, last_name, display_name, state,
  verified_email, accepts_marketing,
  created_at, updated_at, default_address_id, updated_db_at
) values (
  %(customer_id)s, %(email)s, %(phone)s, %(first_name)s, %(last_name)s, %(display_name)s, %(state)s,
  %(verified_email)s, %(accepts_marketing)s,
  %(created_at)s, %(updated_at)s, %(default_address_id)s, now()
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
  created_at = excluded.created_at,
  updated_at = excluded.updated_at,
  default_address_id = excluded.default_address_id,
  updated_db_at = now();
""".strip()

SQL_ADDRESSES_UPSERT = """
insert into public.customer_addresses (
  address_id, customer_id, is_default,
  first_name, last_name, company,
  address1, address2, city, province, province_code,
  country, country_code, zip, phone,
  latitude, longitude,
  updated_db_at
) values (
  %(address_id)s, %(customer_id)s, %(is_default)s,
  %(first_name)s, %(last_name)s, %(company)s,
  %(address1)s, %(address2)s, %(city)s, %(province)s, %(province_code)s,
  %(country)s, %(country_code)s, %(zip)s, %(phone)s,
  %(latitude)s, %(longitude)s,
  now()
)
on conflict (address_id) do update set
  customer_id = excluded.customer_id,
  is_default = excluded.is_default,
  first_name = excluded.first_name,
  last_name = excluded.last_name,
  company = excluded.company,
  address1 = excluded.address1,
  address2 = excluded.address2,
  city = excluded.city,
  province = excluded.province,
  province_code = excluded.province_code,
  country = excluded.country,
  country_code = excluded.country_code,
  zip = excluded.zip,
  phone = excluded.phone,
  latitude = excluded.latitude,
  longitude = excluded.longitude,
  updated_db_at = now();
""".strip()

SQL_GET_CHECKPOINT = """
select checkpoint
from public.etl_state
where pipeline = %(pipeline)s
limit 1;
""".strip()

SQL_UPSERT_CHECKPOINT = """
insert into public.etl_state (pipeline, checkpoint, updated_at)
values (%(pipeline)s, %(checkpoint)s, now())
on conflict (pipeline) do update set
  checkpoint = excluded.checkpoint,
  updated_at = now();
""".strip()


# =========================
# Helpers
# =========================
def iso_to_dt(s: str):
    if not s:
        return None
    # Shopify returns ISO with Z
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def gql(variables: dict):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": TOKEN,
    }
    payload = {"query": QUERY, "variables": variables}
    r = requests.post(GRAPHQL_URL, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(data["errors"])
    return data

def require_env():
    missing = []
    if not SHOP:
        missing.append("SHOPIFY_SHOP")
    if not TOKEN:
        missing.append("SHOPIFY_ADMIN_TOKEN")
    if not POSTGRES_URL:
        missing.append("POSTGRES_URL")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")

def connect_db():
    return psycopg2.connect(POSTGRES_URL)

def get_checkpoint(cur):
    cur.execute(SQL_GET_CHECKPOINT, {"pipeline": PIPELINE_NAME})
    row = cur.fetchone()
    return row[0] if row else None

def set_checkpoint(cur, checkpoint_dt):
    # store as timestamptz
    cur.execute(SQL_UPSERT_CHECKPOINT, {"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt})

def build_query_filter(effective_start_dt: datetime):
    # Shopify filter expects "updated_at:>=YYYY-MM-DDTHH:MM:SSZ"
    z = effective_start_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"updated_at:>={z}"

def normalize_country_code(v):
    if v is None:
        return None
    # enum -> string
    return str(v)

def normalize_float(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


# =========================
# Main
# =========================
def main():
    require_env()

    start_ts = time.time()
    deadline_ts = start_ts + (MAX_RUNTIME_MIN * 60)

    with connect_db() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            checkpoint = get_checkpoint(cur)

            # checkpoint in DB might come as datetime already (timestamptz)
            if isinstance(checkpoint, str):
                checkpoint_dt = iso_to_dt(checkpoint)
            else:
                checkpoint_dt = checkpoint

            if EFFECTIVE_START_ISO:
                effective_start_dt = iso_to_dt(EFFECTIVE_START_ISO)
            else:
                # If no EFFECTIVE_START_ISO, use checkpoint; if none, start from "now - 3650d" (bem antigo)
                effective_start_dt = checkpoint_dt or (datetime.now(timezone.utc) - timedelta(days=3650))

            query_filter = build_query_filter(effective_start_dt)

            print(
                f"Pipeline={PIPELINE_NAME} checkpoint={checkpoint_dt} "
                f"effective_start={effective_start_dt} query='{query_filter}'"
            )

            after = None
            total_customers = 0
            total_addresses = 0

            while True:
                if time.time() > deadline_ts:
                    print("PARTIAL: atingiu limite de tempo. Salvando checkpoint e saindo.")
                    conn.commit()
                    break

                data = gql({"first": PAGE_SIZE, "after": after, "query": query_filter})
                customers = data["data"]["customers"]["edges"]
                page_info = data["data"]["customers"]["pageInfo"]

                if not customers:
                    print("OK: nenhum customer retornado para este filtro (fim).")
                    break

                for edge in customers:
                    c = edge["node"]

                    customer_id = c.get("id")
                    default_addr = c.get("defaultAddress") or {}
                    default_address_id = default_addr.get("id")

                    # upsert customer
                    cur.execute(
                        SQL_CUSTOMERS_UPSERT,
                        {
                            "customer_id": customer_id,
                            "email": c.get("email"),
                            "phone": c.get("phone"),
                            "first_name": c.get("firstName"),
                            "last_name": c.get("lastName"),
                            "display_name": c.get("displayName"),
                            "state": c.get("state"),
                            "verified_email": bool(c.get("verifiedEmail")) if c.get("verifiedEmail") is not None else None,
                            accepts_marketing = (
                                False if customer.get("dataSaleOptOut") is True else True
                            ),
                            "created_at": iso_to_dt(c.get("createdAt")),
                            "updated_at": iso_to_dt(c.get("updatedAt")),
                            "default_address_id": default_address_id,
                        },
                    )
                    total_customers += 1

                    # upsert addresses
                    addr_nodes = ((c.get("addressesV2") or {}).get("nodes")) or []
                    for a in addr_nodes:
                        address_id = a.get("id")
                        is_default = (default_address_id is not None and address_id == default_address_id)

                        cur.execute(
                            SQL_ADDRESSES_UPSERT,
                            {
                                "address_id": address_id,
                                "customer_id": customer_id,
                                "is_default": is_default,
                                "first_name": a.get("firstName"),
                                "last_name": a.get("lastName"),
                                "company": a.get("company"),
                                "address1": a.get("address1"),
                                "address2": a.get("address2"),
                                "city": a.get("city"),
                                "province": a.get("province"),
                                "province_code": a.get("provinceCode"),
                                "country": a.get("country"),
                                "country_code": normalize_country_code(a.get("countryCodeV2")),
                                "zip": a.get("zip"),
                                "phone": a.get("phone"),
                                "latitude": normalize_float(a.get("latitude")),
                                "longitude": normalize_float(a.get("longitude")),
                            },
                        )
                        total_addresses += 1

                conn.commit()

                # atualiza checkpoint com base no último updatedAt visto nesta página
                # (mantém o cursor avançando com segurança)
                last_updated = None
                for edge in customers:
                    u = iso_to_dt(edge["node"].get("updatedAt"))
                    if u and (last_updated is None or u > last_updated):
                        last_updated = u
                if last_updated:
                    set_checkpoint(cur, last_updated)
                    conn.commit()

                if not page_info["hasNextPage"]:
                    print("OK: terminou paginação.")
                    break

                after = page_info["endCursor"]

            print(f"OK: upsert customers={total_customers} addresses={total_addresses}")

if __name__ == "__main__":
    main()
