#!/usr/bin/env python3
import os
import time
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
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "8"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))

# safety window p/ não perder updates no limite do checkpoint
SAFETY_WINDOW_MIN = int(os.getenv("SAFETY_WINDOW_MIN", "120"))

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"

# =========================
# GraphQL (somente campos "seguros")
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
        createdAt
        updatedAt
        defaultAddress { id }
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
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def dt_to_shopify_query(dt: datetime) -> str:
    z = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"updated_at:>={z}"

def gql(variables: dict):
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": TOKEN,
    }
    payload = {"query": QUERY, "variables": variables}
    r = requests.post(GRAPHQL_URL, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    if data.get("errors"):
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

def set_checkpoint(cur, checkpoint_dt: datetime):
    cur.execute(SQL_UPSERT_CHECKPOINT, {"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt})


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

            # checkpoint já pode vir como datetime (timestamptz)
            checkpoint_dt = iso_to_dt(checkpoint) if isinstance(checkpoint, str) else checkpoint

            # safety window: volta um pouco p/ não perder borda
            if checkpoint_dt:
                effective_start_dt = checkpoint_dt - timedelta(minutes=SAFETY_WINDOW_MIN)
            else:
                # primeira execução incremental sem checkpoint: começa "agora - safety"
                effective_start_dt = datetime.now(timezone.utc) - timedelta(minutes=SAFETY_WINDOW_MIN)

            query_filter = dt_to_shopify_query(effective_start_dt)

            print(
                f"Pipeline={PIPELINE_NAME} checkpoint={checkpoint_dt} "
                f"effective_start={effective_start_dt} query='{query_filter}'"
            )

            after = None
            total_customers = 0
            max_updated_seen = checkpoint_dt

            while True:
                if time.time() > deadline_ts:
                    # salva checkpoint do que já vimos
                    if max_updated_seen:
                        set_checkpoint(cur, max_updated_seen)
                        conn.commit()
                    print("PARTIAL: atingiu limite de tempo. Salvando checkpoint e saindo.")
                    break

                data = gql({"first": PAGE_SIZE, "after": after, "query": query_filter})
                edges = data["data"]["customers"]["edges"]
                page_info = data["data"]["customers"]["pageInfo"]

                if not edges:
                    # nada novo
                    if max_updated_seen:
                        set_checkpoint(cur, max_updated_seen)
                        conn.commit()
                    print("OK: nenhum customer retornado para este filtro (fim).")
                    break

                for edge in edges:
                    c = edge["node"]
                    customer_id = c.get("id")
                    default_addr = c.get("defaultAddress") or {}
                    default_address_id = default_addr.get("id")

                    updated_at = iso_to_dt(c.get("updatedAt"))
                    if updated_at and (max_updated_seen is None or updated_at > max_updated_seen):
                        max_updated_seen = updated_at

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
                            # sem campo confiável na sua API atual -> deixa NULL
                            "accepts_marketing": None,
                            "created_at": iso_to_dt(c.get("createdAt")),
                            "updated_at": updated_at,
                            "default_address_id": default_address_id,
                        },
                    )
                    total_customers += 1

                conn.commit()

                # checkpoint por página (mais seguro)
                if max_updated_seen:
                    set_checkpoint(cur, max_updated_seen)
                    conn.commit()

                if not page_info["hasNextPage"]:
                    print("OK: terminou paginação.")
                    break

                after = page_info["endCursor"]

            print(f"OK: upsert customers={total_customers}")

if __name__ == "__main__":
    main()