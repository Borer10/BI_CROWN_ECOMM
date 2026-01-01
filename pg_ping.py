import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

db_url = os.getenv("POSTGRES_URL")
if not db_url:
    raise SystemExit("POSTGRES_URL não encontrada no .env")

engine = create_engine(db_url, pool_pre_ping=True)

with engine.connect() as conn:
    result = conn.execute(text("select now() as now")).mappings().first()
    print("✅ Conectado ao Supabase!")
    print("Agora:", result["now"])
