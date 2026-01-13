#!/usr/bin/env python3
import os
import time
from datetime import datetime, timezone, timedelta

import psycopg2
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
from google.api_core.exceptions import InvalidArgument

# =========================
# ENV
# =========================
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")  # ex: "290124262"
POSTGRES_URL = os.getenv("POSTGRES_URL")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "ga4_daily_funnel_channel")
SAFETY_DAYS = int(os.getenv("SAFETY_DAYS", "3"))
EFFECTIVE_START_DATE = os.getenv("EFFECTIVE_START_DATE", "").strip()

MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))
START_TIME = time.time()

# GA4 property timezone já está São Paulo; a dimensão "date" vem fechada no TZ da propriedade.
# Gravamos como DATE (sem timezone) para bater com o "dia GA4".
# =========================


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

DDL_TABLE = """
create table if not exists public.ga4_daily_funnel_channel (
  date date not null,
  source text,
  medium text,
  campaign text,

  sessions bigint,
  add_to_carts bigint,
  checkouts bigint,
  transactions bigint,
  revenue numeric,

  updated_db_at timestamptz not null default now(),
  primary key (date, source, medium, campaign)
);
"""

UPSERT_CHANNEL = """
insert into public.ga4_daily_funnel_channel (
  date, source, medium, campaign,
  sessions, add_to_carts, checkouts, transactions, revenue,
  updated_db_at
) values (
  %(date)s, %(source)s, %(medium)s, %(campaign)s,
  %(sessions)s, %(add_to_carts)s, %(checkouts)s, %(transactions)s, %(revenue)s,
  now()
)
on conflict (date, source, medium, campaign) do update set
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
    - Se EFFECTIVE_START_DATE setado: começa nele
    - Senão, se não tem checkpoint: começa 3650 dias atrás
    - Sempre reprocessa SAFETY_DAYS para trás (ajustes tardios do GA4)
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


def fetch_daily_funnel_channel(client: BetaAnalyticsDataClient, start_date: str, end_date: str):
    """
    Dimensões:
      - date
      - sessionSource, sessionMedium, sessionCampaignName
    Métricas:
      - sessions, addToCarts, checkouts, transactions, totalRevenue
    """
    dimensions = ["date", "sessionSource", "sessionMedium", "sessionCampaignName"]
    metrics = ["sessions", "addToCarts", "checkouts", "transactions", "totalRevenue"]

    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=250000,  # se estourar, depois a gente pagina
    )
    return client.run_report(req), dimensions, metrics


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
                f"GA4 daily_funnel channel | property={GA4_PROPERTY_ID} "
                f"checkpoint={checkpoint_dt} window={start_date}..{end_date} safety_days={SAFETY_DAYS}"
            )

            if should_stop_soon():
                print("PARTIAL: max runtime antes de chamar GA4.")
                return

            client = ga4_client()

            # ✅ Validador (INVALID_ARGUMENT) com métricas/dimensões claras
            try:
                resp, dims, mets = fetch_daily_funnel_channel(client, start_date, end_date)
            except InvalidArgument as e:
                print("\n🚫 GA4 INVALID_ARGUMENT (provavelmente métrica/dimensão inválida)")
                print(f"- Property: {GA4_PROPERTY_ID}")
                print(f"- Dimensões pedidas: {['date','sessionSource','sessionMedium','sessionCampaignName']}")
                print(f"- Métricas pedidas: {['sessions','addToCarts','checkouts','transactions','totalRevenue']}")
                msg = str(e)
                print(f"- Detalhe do GA4: {msg}\n")
                print("Dica: confira os nomes no schema oficial da GA4 Data API.")
                raise SystemExit(1)

            rows = resp.rows or []
            if not rows:
                new_cp = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
                set_checkpoint(cur, new_cp)
                conn.commit()
                print(f"OK: 0 linhas. checkpoint={new_cp.isoformat()}")
                return

            upserted = 0
            for r in rows:
                if should_stop_soon():
                    print("PARTIAL: atingiu max runtime durante upsert.")
                    break

                # dimensões
                date_str = parse_ga4_date(r.dimension_values[0].value)
                source = r.dimension_values[1].value or None
                medium = r.dimension_values[2].value or None
                campaign = r.dimension_values[3].value or None

                # métricas na ordem definida
                m = [mv.value for mv in r.metric_values]
                sessions = int(float(m[0])) if m[0] else 0
                add_to_carts = int(float(m[1])) if m[1] else 0
                checkouts = int(float(m[2])) if m[2] else 0
                transactions = int(float(m[3])) if m[3] else 0
                revenue = float(m[4]) if m[4] else 0.0

                cur.execute(
                    UPSERT_CHANNEL,
                    {
                        "date": date_str,
                        "source": source,
                        "medium": medium,
                        "campaign": campaign,
                        "sessions": sessions,
                        "add_to_carts": add_to_carts,
                        "checkouts": checkouts,
                        "transactions": transactions,
                        "revenue": revenue,
                    },
                )
                upserted += 1

            conn.commit()

            # checkpoint avança para o end_date (janela varrida)
            new_cp = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
            set_checkpoint(cur, new_cp)
            conn.commit()
            print(f"OK: upsert {upserted} linhas (date+source/medium/campaign). checkpoint={new_cp.isoformat()}")


if __name__ == "__main__":
    main()