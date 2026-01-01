import os
import requests
from dotenv import load_dotenv

load_dotenv()

SHOP = os.getenv("SHOPIFY_SHOP")
TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-01")

url = f"https://{SHOP}/admin/api/{VERSION}/graphql.json"
headers = {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json",
}

query = """
query {
  shop {
    name
    myshopifyDomain
  }
}
"""

response = requests.post(
    url,
    headers=headers,
    json={"query": query},
    timeout=60
)

print("HTTP Status:", response.status_code)
print(response.text)
