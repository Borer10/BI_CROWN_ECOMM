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
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()
POSTGRES_URL = os.getenv("POSTGRES_URL", "").strip()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "ga4_landing_pages_daily")
SAFETY_DAYS = int(os.getenv("SAFETY_DAYS", "3"))
EFFECTIVE_START_DATE = os.getenv("EFFECTIVE_START_DATE", "").strip()

MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))
START_TIME = time.time()

# =========================
# DDL / SQL
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

DDL_TABLE = """
create table if not exists public.ga4_landing_pages_daily (
  date date not null,
  landing_page text not null,

  active_users bigint,
  new_users bigint,
  sessions bigint,
  engaged_sessions bigint,

  engagement_rate numeric,
  avg_engagement_time_sec numeric,

  key_events numeric,
  revenue numeric,

  updated_db_at timestamptz not null default now(),

  primary key (date, landing_page)
);
"""

UPSERT = """
insert into public.ga4_landing_pages_daily (
  date, landing_page,
  active_users, new_users, sessions, engaged_sessions,
  engagement_rate, avg_engagement_time_sec,
  key_events, revenue,
  updated_db_at
) values (
  %(date)s, %(landing_page)s,
  %(active_users)s, %(new_users)s, %(sessions)s, %(engaged_sessions)s,
  %(engagement_rate)s, %(avg_engagement_time_sec)s,
  %(key_events)s, %(revenue)s,
  now()
)
on conflict (date, landing_page) do update set
  active_users = excluded.active_users,
  new_users = excluded.new_users,
  sessions = excluded.sessions,
  engaged_sessions = excluded.engaged_sessions,
  engagement_rate = excluded.engagement_rate,
  avg_engagement_time_sec = excluded.avg_engagement_time_sec,
  key_events = excluded.key_events,
  revenue = excluded.revenue,
  updated_db_at = now();
"""

# =========================
# Helpers
# =========================
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
    # GA4 date: YYYYMMDD
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"

def build_date_window(checkpoint_dt):
    now_utc = datetime.now(timezone.utc)

    if EFFECTIVE_START_DATE:
        start = datetime.fromisoformat(EFFECTIVE_START_DATE).replace(tzinfo=timezone.utc)
    else:
        if checkpoint_dt is None:
            start = now_utc - timedelta(days=3650)
        else:
            start = checkpoint_dt - timedelta(days=SAFETY_DAYS)

    end = now_utc - timedelta(days=1)  # até ontem (dia fechado)

    if start.date() > end.date():
        return None

    return start.date(), end.date()

def ga4_client():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)

def fetch_report(client, start_date: str, end_date: str):
    # Dimensões: date + landingPagePlusQueryString (melhor proxy de landing)
    # Vamos salvar só o path+query; depois se quiser a gente normaliza pra path-only.
    dimensions = [Dimension(name="date"), Dimension(name="landingPagePlusQueryString")]

    metrics = [
        Metric(name="activeUsers"),
        Metric(name="newUsers"),
        Metric(name="sessions"),
        Metric(name="engagedSessions"),
        Metric(name="engagementRate"),
        Metric(name="averageSessionDuration"),  # segundos (numérico)
        Metric(name="keyEvents"),
        Metric(name="totalRevenue"),
    ]

    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=dimensions,
        metrics=metrics,
        limit=250000,  # landing pages pode dar bastante linha; se estourar, a gente pagina depois
    )
    return client.run_report(req), [d.name for d in dimensions], [m.name for m in metrics]

def ga4_invalid_argument_hint(dimensions, metrics, exc: Exception):
    print("⛔ GA4 INVALID_ARGUMENT (provavelmente métrica/dimensão inválida)")
    print(f"- Property: {GA4_PROPERTY_ID}")
    print(f"- Dimensões pedidas: {dimensions}")
    print(f"- Métricas pedidas: {metrics}")
    print(f"- Detalhe do GA4: {exc}")
    print("")
    print("Dica: confira os nomes no schema oficial do GA4 Data API.")
    raise SystemExit(1)

def safe_int(v: str) -> int:
    if v is None or v == "":
        return 0
    return int(float(v))

def safe_float(v: str) -> float:
    if v is None or v == "":
        return 0.0
    return float(v)

def main():
    require_env()

    with connect_db() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
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
                f"GA4 landing_pages_daily | property={GA4_PROPERTY_ID} "
                f"checkpoint={checkpoint_dt} window={start_date}..{end_date} safety_days={SAFETY_DAYS}"
            )

            if should_stop_soon():
                print("PARTIAL: max runtime antes de chamar GA4.")
                return

            client = ga4_client()
            try:
                resp, dims, mets = fetch_report(client, start_date, end_date)
            except InvalidArgument as e:
                ga4_invalid_argument_hint(
                    ["date", "landingPagePlusQueryString"],
                    ["activeUsers", "newUsers", "sessions", "engagedSessions", "engagementRate", "averageSessionDuration", "keyEvents", "totalRevenue"],
                    e,
                )

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

                date_raw = r.dimension_values[0].value
                landing = r.dimension_values[1].value or ""

                date_str = parse_ga4_date(date_raw)
                mv = [x.value for x in r.metric_values]

                payload = {
                    "date": date_str,
                    "landing_page": landing[:2000],  # segurança
                    "active_users": safe_int(mv[0]),
                    "new_users": safe_int(mv[1]),
                    "sessions": safe_int(mv[2]),
                    "engaged_sessions": safe_int(mv[3]),
                    "engagement_rate": safe_float(mv[4]),
                    "avg_engagement_time_sec": safe_float(mv[5]),
                    "key_events": safe_float(mv[6]),
                    "revenue": safe_float(mv[7]),
                }

                cur.execute(UPSERT, payload)
                upserted += 1
                last_date_seen = date_str

            conn.commit()

            if last_date_seen:
                new_cp = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
                set_checkpoint(cur, new_cp)
                conn.commit()
                print(f"OK: upsert {upserted} linhas (date+landing_page). checkpoint={new_cp.isoformat()}")
            else:
                print(f"OK: upsert {upserted} linhas. checkpoint não alterado (sem last_date_seen).")

if __name__ == "__main__":
    main()