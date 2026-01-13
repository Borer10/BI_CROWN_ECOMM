#!/usr/bin/env python3
import os
import time
from datetime import datetime, timezone, timedelta

import psycopg2
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account
from google.api_core.exceptions import InvalidArgument

# =========================
# ENV
# =========================
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")  # ex: "290124262"
POSTGRES_URL = os.getenv("POSTGRES_URL")

# JSON do service account vem via Secret e é escrito em arquivo pelo workflow
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "ga4_daily_funnel_general")

# Quantos dias “para trás” reprocessar sempre (safety window) para pegar ajustes tardios do GA4
SAFETY_DAYS = int(os.getenv("SAFETY_DAYS", "3"))

# Se quiser forçar backfill manual, use:
# EFFECTIVE_START_DATE="2025-12-01"
EFFECTIVE_START_DATE = os.getenv("EFFECTIVE_START_DATE", "").strip()

# Limite de tempo opcional (0 = sem limite)
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))
START_TIME = time.time()

# =========================
# Postgres (etl_state + tabela destino)
# =========================
DDL_STATE = """
create table if not exists public.etl_state (
  pipeline text primary key,
  checkpoint timestamptz not null,
  updated_at timestamptz not null default now()
);
"""

SQL_GET_STATE = """
select checkpoint
from public.etl_state
where pipeline = %(pipeline)s
limit 1;
"""

SQL_UPSERT_STATE = """
insert into public.etl_state (pipeline, checkpoint, updated_at)
values (%(pipeline)s, %(checkpoint)s, now())
on conflict (pipeline) do update
set checkpoint = excluded.checkpoint,
    updated_at = now();
"""

# (idempotente) — mantém compatível com seu schema atual
DDL_TABLE = """
create table if not exists public.ga4_daily_funnel (
  date date primary key,
  active_users bigint,
  sessions bigint,
  add_to_carts bigint,
  checkouts bigint,
  transactions bigint,
  revenue numeric,
  updated_db_at timestamptz not null default now()
);
"""

UPSERT_FUNNEL = """
insert into public.ga4_daily_funnel (
  date, active_users, sessions, add_to_carts, checkouts, transactions, revenue, updated_db_at
) values (
  %(date)s, %(active_users)s, %(sessions)s, %(add_to_carts)s, %(checkouts)s, %(transactions)s, %(revenue)s, now()
)
on conflict (date) do update set
  active_users = excluded.active_users,
  sessions = excluded.sessions,
  add_to_carts = excluded.add_to_carts,
  checkouts = excluded.checkouts,
  transactions = excluded.transactions,
  revenue = excluded.revenue,
  updated_db_at = now();
"""


def require_env():
    missing = []
    if not GA4_PROPERTY_ID:
        missing.append("GA4_PROPERTY_ID")
    if not POSTGRES_URL:
        missing.append("POSTGRES_URL")
    if not GOOGLE_APPLICATION_CREDENTIALS:
        missing.append("GOOGLE_APPLICATION_CREDENTIALS (path do json)")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")


def should_stop_soon() -> bool:
    if MAX_RUNTIME_MIN <= 0:
        return False
    return ((time.time() - START_TIME) / 60.0) >= MAX_RUNTIME_MIN


def connect_db():
    return psycopg2.connect(POSTGRES_URL)


def get_checkpoint(cur):
    cur.execute(SQL_GET_STATE, {"pipeline": PIPELINE_NAME})
    row = cur.fetchone()
    return row[0] if row else None


def set_checkpoint(cur, checkpoint_dt: datetime):
    cur.execute(SQL_UPSERT_STATE, {"pipeline": PIPELINE_NAME, "checkpoint": checkpoint_dt})


def parse_ga4_date(s: str) -> str:
    # GA4 "date" vem como YYYYMMDD
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


def build_date_window(checkpoint_dt):
    """
    Estratégia:
    - Se EFFECTIVE_START_DATE setado: começa nele
    - Senão, se não tem checkpoint: começa bem no passado (ex: 3650 dias)
    - Sempre reprocessa SAFETY_DAYS para trás
    - Vai até ontem (dia fechado)
    """
    now_utc = datetime.now(timezone.utc)

    if EFFECTIVE_START_DATE:
        start = datetime.fromisoformat(EFFECTIVE_START_DATE).replace(tzinfo=timezone.utc)
    else:
        if checkpoint_dt is None:
            start = now_utc - timedelta(days=3650)
        else:
            start = checkpoint_dt - timedelta(days=SAFETY_DAYS)

    end = now_utc - timedelta(days=1)

    if start.date() > end.date():
        return None

    return start.date(), end.date()


def ga4_client():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def fetch_daily_funnel(client: BetaAnalyticsDataClient, start_date: str, end_date: str):
    dimensions = ["date"]
    metrics = ["activeUsers", "sessions", "addToCarts", "checkouts", "transactions", "totalRevenue"]

    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
    )
    return req, dimensions, metrics, client.run_report(req)


def main():
    require_env()

    with connect_db() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # garante tabelas
            cur.execute(DDL_STATE)
            cur.execute(DDL_TABLE)
            conn.commit()

            checkpoint_dt = get_checkpoint(cur)

            window = build_date_window(checkpoint_dt)
            if window is None:
                print("OK: nada para atualizar (janela vazia).")
                return

            start_d, end_d = window
            start_date = start_d.isoformat()
            end_date = end_d.isoformat()

            print(
                f"GA4 daily_funnel general | property={GA4_PROPERTY_ID} "
                f"checkpoint={checkpoint_dt} window={start_date}..{end_date} safety_days={SAFETY_DAYS}"
            )

            client = ga4_client()
            if should_stop_soon():
                print("PARTIAL: max runtime antes de chamar GA4.")
                return

            # ======= chamada GA4 com validador =======
            try:
                _, dims, mets, resp = fetch_daily_funnel(client, start_date, end_date)
            except InvalidArgument as e:
                msg = str(e)
                print("\n⛔ GA4 INVALID_ARGUMENT (provavelmente métrica/dimensão inválida)")
                print(f"- Property: {GA4_PROPERTY_ID}")
                print(f"- Dimensões pedidas: {dims}")
                print(f"- Métricas pedidas: {mets}")
                print(f"- Detalhe do GA4: {msg}\n")
                print("Dica: confira os nomes no schema oficial do GA4 Data API.")
                raise SystemExit(1)

            rows = resp.rows or []
            if not rows:
                new_cp = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
                set_checkpoint(cur, new_cp)
                conn.commit()
                print(f"OK: 0 linhas. checkpoint={new_cp.isoformat()}")
                return

            upserted = 0
            last_date_seen = None

            for r in rows:
                if should_stop_soon():
                    print("PARTIAL: atingiu max runtime durante upsert.")
                    break

                date_raw = r.dimension_values[0].value  # YYYYMMDD
                date_str = parse_ga4_date(date_raw)      # YYYY-MM-DD

                m = [mv.value for mv in r.metric_values]
                # ordem: activeUsers, sessions, addToCarts, checkouts, transactions, totalRevenue
                active_users = int(float(m[0])) if m[0] else 0
                sessions = int(float(m[1])) if m[1] else 0
                add_to_carts = int(float(m[2])) if m[2] else 0
                checkouts = int(float(m[3])) if m[3] else 0
                transactions = int(float(m[4])) if m[4] else 0
                revenue = float(m[5]) if m[5] else 0.0

                cur.execute(
                    UPSERT_FUNNEL,
                    {
                        "date": date_str,
                        "active_users": active_users,
                        "sessions": sessions,
                        "add_to_carts": add_to_carts,
                        "checkouts": checkouts,
                        "transactions": transactions,
                        "revenue": revenue,
                    },
                )
                upserted += 1
                last_date_seen = date_str

            conn.commit()

            if last_date_seen:
                new_cp = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
                set_checkpoint(cur, new_cp)
                conn.commit()
                print(f"OK: upsert {upserted} dias. checkpoint={new_cp.isoformat()}")
            else:
                print(f"OK: upsert {upserted} dias. checkpoint não alterado (sem last_date_seen).")


if __name__ == "__main__":
    main()