#!/usr/bin/env python3
import os
import time
from datetime import datetime, timezone, timedelta

import psycopg2
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest
)
from google.oauth2 import service_account
from google.api_core.exceptions import InvalidArgument

# =========================
# ENV
# =========================
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
POSTGRES_URL = os.getenv("POSTGRES_URL")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

PIPELINE_NAME = os.getenv("PIPELINE_NAME", "ga4_events_daily")
SAFETY_DAYS = int(os.getenv("SAFETY_DAYS", "3"))
MAX_RUNTIME_MIN = int(os.getenv("MAX_RUNTIME_MIN", "0"))
START_TIME = time.time()

EVENTS = [
    "page_view",
    "view_item",
    "add_to_cart",
    "begin_checkout",
    "purchase",
]

# =========================
# SQL
# =========================
DDL_STATE = """
create table if not exists public.etl_state (
  pipeline text primary key,
  checkpoint timestamptz not null,
  updated_at timestamptz not null default now()
);
"""

DDL_TABLE = """
create table if not exists public.ga4_events_daily (
  date date not null,
  event_name text not null,
  event_count bigint,
  total_users bigint,
  updated_db_at timestamptz not null default now(),
  primary key (date, event_name)
);
"""

UPSERT_SQL = """
insert into public.ga4_events_daily (
  date, event_name, event_count, total_users, updated_db_at
) values (
  %(date)s, %(event_name)s, %(event_count)s, %(total_users)s, now()
)
on conflict (date, event_name) do update set
  event_count = excluded.event_count,
  total_users = excluded.total_users,
  updated_db_at = now();
"""

SQL_GET_STATE = """
select checkpoint
from public.etl_state
where pipeline = %(pipeline)s;
"""

SQL_SET_STATE = """
insert into public.etl_state (pipeline, checkpoint, updated_at)
values (%(pipeline)s, %(checkpoint)s, now())
on conflict (pipeline) do update
set checkpoint = excluded.checkpoint,
    updated_at = now();
"""

# =========================
# Helpers
# =========================
def should_stop():
    if MAX_RUNTIME_MIN <= 0:
        return False
    return (time.time() - START_TIME) / 60 >= MAX_RUNTIME_MIN


def ga4_client():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def parse_date(s):
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


# =========================
# Main
# =========================
def main():
    with psycopg2.connect(POSTGRES_URL) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(DDL_STATE)
            cur.execute(DDL_TABLE)
            conn.commit()

            cur.execute(SQL_GET_STATE, {"pipeline": PIPELINE_NAME})
            row = cur.fetchone()
            checkpoint = row[0] if row else None

            now_utc = datetime.now(timezone.utc)
            if checkpoint:
                start = checkpoint - timedelta(days=SAFETY_DAYS)
            else:
                start = now_utc - timedelta(days=3650)

            end = now_utc - timedelta(days=1)
            if start.date() > end.date():
                print("OK: nada para atualizar.")
                return

            print(
                f"GA4 events_daily | property={GA4_PROPERTY_ID} "
                f"window={start.date()}..{end.date()} safety_days={SAFETY_DAYS}"
            )

            client = ga4_client()

            try:
                req = RunReportRequest(
                    property=f"properties/{GA4_PROPERTY_ID}",
                    date_ranges=[DateRange(start_date=start.date().isoformat(), end_date=end.date().isoformat())],
                    dimensions=[
                        Dimension(name="date"),
                        Dimension(name="eventName"),
                    ],
                    metrics=[
                        Metric(name="eventCount"),
                        Metric(name="totalUsers"),
                    ],
                    dimension_filter={
                        "filter": {
                            "field_name": "eventName",
                            "in_list_filter": {"values": EVENTS},
                        }
                    },
                )

                resp = client.run_report(req)

            except InvalidArgument as e:
                print("GA4 INVALID_ARGUMENT")
                print("Eventos pedidos:", EVENTS)
                print("Erro:", e.message)
                raise

            rows = resp.rows or []
            upserted = 0

            for r in rows:
                if should_stop():
                    print("PARTIAL: max runtime atingido.")
                    break

                date = parse_date(r.dimension_values[0].value)
                event = r.dimension_values[1].value
                event_count = int(float(r.metric_values[0].value))
                users = int(float(r.metric_values[1].value))

                cur.execute(
                    UPSERT_SQL,
                    {
                        "date": date,
                        "event_name": event,
                        "event_count": event_count,
                        "total_users": users,
                    },
                )
                upserted += 1

            cur.execute(
                SQL_SET_STATE,
                {"pipeline": PIPELINE_NAME, "checkpoint": end.replace(tzinfo=timezone.utc)},
            )
            conn.commit()

            print(f"OK: upsert {upserted} linhas. checkpoint={end.isoformat()}")


if __name__ == "__main__":
    main()