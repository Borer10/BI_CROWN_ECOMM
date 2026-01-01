import os
from dotenv import load_dotenv

load_dotenv()

print(os.getenv("SHOPIFY_SHOP"))
print(os.getenv("SHOPIFY_ADMIN_TOKEN")[:10])