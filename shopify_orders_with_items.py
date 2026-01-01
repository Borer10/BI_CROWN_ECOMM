import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

SHOP = os.getenv("SHOPIFY_SHOP")
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")

if not SHOP or not TOKEN:
    raise SystemExit("Faltou SHOPIFY_SHOP ou SHOPIFY_ADMIN_TOKEN no .env")

URL = f"https://{SHOP}/admin/api/{VERSION}/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

QUERY = """
query Orders($first: Int!, $after: String, $query: String) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
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
        subtotalPriceSet { shopMoney { amount currencyCode } }
        totalShippingPriceSet { shopMoney { amount currencyCode } }
        totalTaxSet { shopMoney { amount currencyCode } }

        customer { id email firstName lastName }

        lineItems(first: 250) {
          edges {
            node {
              id
              name
              quantity
              sku
              originalUnitPriceSet { shopMoney { amount currencyCode } }
              variant { id title }
              product { id title }
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

def main():
    # TODOS os pedidos desde 01/01/2025
    query_filter = "created_at:>=2025-01-01"

    orders_rows = []
    items_rows = []

    after = None
    while True:
        data = gql({"first": 100, "after": after, "query": query_filter})
        edges = data["orders"]["edges"]

        for e in edges:
            o = e["node"]
            customer = o.get("customer") or {}

            orders_rows.append({
                "order_id": o["id"],
                "order_name": o["name"],
                "created_at": o["createdAt"],
                "updated_at": o["updatedAt"],
                "financial_status": o.get("displayFinancialStatus"),
                "fulfillment_status": o.get("displayFulfillmentStatus"),
                "currency": o.get("currencyCode"),
                "total_price": money(o.get("totalPriceSet") or {}),
                "subtotal_price": money(o.get("subtotalPriceSet") or {}),
                "shipping_price": money(o.get("totalShippingPriceSet") or {}),
                "tax_price": money(o.get("totalTaxSet") or {}),
                "customer_id": customer.get("id"),
                "customer_email": customer.get("email"),
                "customer_first_name": customer.get("firstName"),
                "customer_last_name": customer.get("lastName"),
            })

            li_edges = ((o.get("lineItems") or {}).get("edges")) or []
            for li in li_edges:
                n = li["node"]
                variant = n.get("variant") or {}
                product = n.get("product") or {}

                items_rows.append({
                    "order_id": o["id"],
                    "order_name": o["name"],
                    "order_created_at": o["createdAt"],
                    "line_item_id": n["id"],
                    "line_item_name": n.get("name"),
                    "sku": n.get("sku"),
                    "quantity": n.get("quantity"),
                    "unit_price": money(n.get("originalUnitPriceSet") or {}),
                    "currency": (n.get("originalUnitPriceSet") or {}).get("shopMoney", {}).get("currencyCode"),
                    "variant_id": variant.get("id"),
                    "variant_title": variant.get("title"),
                    "product_id": product.get("id"),
                    "product_title": product.get("title"),
                })

        page = data["orders"]["pageInfo"]
        if not page["hasNextPage"]:
            break
        after = page["endCursor"]
        time.sleep(0.35)

    df_orders = pd.DataFrame(orders_rows)
    df_items = pd.DataFrame(items_rows)

    df_orders.to_csv("shopify_orders_since_2025-01-01.csv", index=False)
    df_items.to_csv("shopify_order_items_since_2025-01-01.csv", index=False)

    print(f"OK: {len(df_orders)} pedidos -> shopify_orders_since_2025-01-01.csv")
    print(f"OK: {len(df_items)} itens -> shopify_order_items_since_2025-01-01.csv")

if __name__ == "__main__":
    main()
