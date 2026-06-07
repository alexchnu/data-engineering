from __future__ import annotations

from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd


@dag(
    dag_id="etl_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 5, 22),
    catchup=False,
    max_active_runs=1,
    tags=["lab6", "etl"],
)
def etl_pipeline():
    """Simple ETL: Extract nested JSON → Transform (flatten) → Load (DataFrame)."""

    @task()
    def extract() -> list[dict]:
        """Generate sample nested JSON records (bike-trip style data)."""
        return [
            {
                "trip_id": "25223640",
                "start_time": "2019-10-01 00:01:39",
                "duration": 940,
                "station": {
                    "from": {"id": 20, "name": "Sheffield Ave & Kingsbury St"},
                    "to":   {"id": 309, "name": "Leavitt St & Armitage Ave"},
                },
                "rider": {"usertype": "Subscriber", "gender": "Male", "birthyear": 1987},
            },
            {
                "trip_id": "25223641",
                "start_time": "2019-10-01 00:02:16",
                "duration": 258,
                "station": {
                    "from": {"id": 19, "name": "Throop (Loomis) St & Taylor St"},
                    "to":   {"id": 241, "name": "Morgan St & Polk St"},
                },
                "rider": {"usertype": "Subscriber", "gender": "Male", "birthyear": 1998},
            },
            {
                "trip_id": "25223642",
                "start_time": "2019-10-01 00:04:32",
                "duration": 850,
                "station": {
                    "from": {"id": 84, "name": "Milwaukee Ave & Grand Ave"},
                    "to":   {"id": 199, "name": "Wabash Ave & Grand Ave"},
                },
                "rider": {"usertype": "Customer", "gender": "Female", "birthyear": 1991},
            },
        ]

    @task()
    def transform(raw: list[dict]) -> list[dict]:
        """Flatten nested JSON — collapse station/rider sub-objects to top level."""
        flat_records = []
        for rec in raw:
            flat = {
                "trip_id":          rec["trip_id"],
                "start_time":       rec["start_time"],
                "duration_seconds": rec["duration"],
                "from_station_id":  rec["station"]["from"]["id"],
                "from_station":     rec["station"]["from"]["name"],
                "to_station_id":    rec["station"]["to"]["id"],
                "to_station":       rec["station"]["to"]["name"],
                "usertype":         rec["rider"]["usertype"],
                "gender":           rec["rider"]["gender"],
                "birthyear":        rec["rider"]["birthyear"],
            }
            flat_records.append(flat)
        return flat_records

    @task()
    def load(flat: list[dict]) -> None:
        """Build a DataFrame from the flat records and print to the task log."""
        df = pd.DataFrame(flat)
        print("\n=== Loaded DataFrame ===")
        print(df.to_string(index=False))
        print(f"\nRows: {len(df)}, Columns: {list(df.columns)}")

    load(transform(extract()))


etl_pipeline()
