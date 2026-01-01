import os, json, time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

SHOP = os.getenv("SHOPIFY_SHOP")
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")

URL = f"https://{SHOP}/admin/api/{VERSION}/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

STATE_FILE = "state.json"

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

def load_state():
    if not os.path.exists(STATE_FILE):
        return {"last_updated_at": "2025-01-01T00:00:00Z"}  # backfill inicial
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(last_updated_at: str):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_updated_at": last_updated_at}, f)

def now_iso_z():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def main():
    state = load_state()
    last = state["last_updated_at"]

    # “Janela de segurança” simples: volta 2 horas para não perder atualizações na borda
    # (depois a gente garante idempotência no upsert)
    query_filter = f"updated_at:>={last}"

    orders_rows = []
    items_rows = []

    after = None
    max_seen_updated_at = last

    while True:
        data = gql({"first": 100, "after": after, "query": query_filter})
        edges = data["orders"]["edges"]

        for e in edges:
            o = e["node"]
            cust = o.get("customer") or {}

            # track maior updatedAt visto
            if o["updatedAt"] > max_seen_updated_at:
                max_seen_updated_at = o["updatedAt"]

            orders_rows.append({
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

            for li in ((o.get("lineItems") or {}).get("edges") or []):
                n = li["node"]
                items_rows.append({
                    "line_item_id": n["id"],
                    "order_id": o["id"],
                    "order_updated_at": o["updatedAt"],
                    "sku": n.get("sku"),
                    "name": n.get("name"),
                    "quantity": n.get("quantity"),
                    "unit_price": money(n.get("originalUnitPriceSet") or {}),
                    "currency": (n.get("originalUnitPriceSet") or {}).get("shopMoney", {}).get("currencyCode"),
                })

        page = data["orders"]["pageInfo"]
        if not page["hasNextPage"]:
            break
        after = page["endCursor"]
        time.sleep(0.35)

    df_orders = pd.DataFrame(orders_rows)
    df_items = pd.DataFrame(items_rows)

    ts = now_iso_z().replace(":", "").replace("-", "")
    df_orders.to_csv(f"orders_incremental_{ts}.csv", index=False)
    df_items.to_csv(f"order_items_incremental_{ts}.csv", index=False)

    # checkpoint só avança se deu tudo certo
    save_state(max_seen_updated_at)

    print(f"OK: {len(df_orders)} pedidos, {len(df_items)} itens. checkpoint={max_seen_updated_at}")

if __name__ == "__main__":
    main()

