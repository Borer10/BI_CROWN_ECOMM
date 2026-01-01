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

url = f"https://{SHOP}/admin/api/{VERSION}/graphql.json"
headers = {"X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json"}

QUERY = """
query Orders($first: Int!, $after: String, $query: String) {
  orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        name
        createdAt
        displayFinancialStatus
        displayFulfillmentStatus
        currencyCode
        totalPriceSet { shopMoney { amount currencyCode } }
        customer { email }
      }
    }
  }
}
"""

def gql(variables):
    r = requests.post(url, headers=headers, json={"query": QUERY, "variables": variables}, timeout=60)
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
    
    query_filter = "created_at:>=2025-01-01"

    rows = []
    after = None

    while True:
        data = gql({"first": 100, "after": after, "query": query_filter})
        edges = data["orders"]["edges"]

        for e in edges:
            o = e["node"]
            rows.append({
                "order_id": o["id"],
                "order_name": o["name"],
                "created_at": o["createdAt"],
                "financial_status": o.get("displayFinancialStatus"),
                "fulfillment_status": o.get("displayFulfillmentStatus"),
                "currency": o.get("currencyCode"),
                "total_price": money(o.get("totalPriceSet") or {}),
                "customer_email": (o.get("customer") or {}).get("email"),
            })

        page = data["orders"]["pageInfo"]
        if not page["hasNextPage"]:
            break
        after = page["endCursor"]
        time.sleep(0.35)

    df = pd.DataFrame(rows)
    out = "shopify_orders_basic_since_2025-01-01.csv"
    df.to_csv(out, index=False)
    print(f"OK: {len(df)} pedidos -> {out}")

if __name__ == "__main__":
    main()
