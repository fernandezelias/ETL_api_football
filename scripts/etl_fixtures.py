# etl_fixtures.py
# Script base para orquestación con Prefect

# --- Librerías estándar ---
import os
import time  # pausa opcional entre requests (rate limit)
from datetime import datetime, timedelta
from configparser import ConfigParser

# --- Librerías externas ---
import requests
import pandas as pd

# --- Prefect ---
from prefect import task, flow
from prefect.runtime import flow_run

# --- Módulos propios ---
from etl_utils import (
    save_new_data_as_delta,
    read_most_recent_partition,
    read_all_from_delta,
    rename_fixture_id,
    normalize_score_cols_to_float,
    add_event_date_from_fixture_date,
    standardize_column_names,
    format_datetime_columns,
    drop_irrelevant_cols,
    add_match_winner,
    add_total_goals,
    cast_column_types,
    cast_gold_categoricals,
    ensure_event_date_utc,
)

# --- Configuración de credenciales ---
parser = ConfigParser()
parser.read("pipeline.conf")
BASE_URL = parser["api-credentials"]["base_url"]
API_KEY  = parser["api-credentials"]["api_key"]

# --- Paths del datalake ---
DATALAKE_ROOT   = "data/etl_datalake"
BRONZE_FIXTURES = f"{DATALAKE_ROOT}/bronze/api_football/fixtures"
SILVER_FIXTURES = f"{DATALAKE_ROOT}/silver/api_football/fixtures"
GOLD_FIXTURES   = f"{DATALAKE_ROOT}/gold/api_football/fixtures"
EXPORTS_DIR     = f"{DATALAKE_ROOT}/exports"


# -------------------- Tasks --------------------

@task(
    retries=3,
    retry_delay_seconds=60,
    task_run_name="get_data-{endpoint}"
)
def extract_data(endpoint: str, params: dict = None) -> list:
    """Extrae datos crudos desde la API-Football para el endpoint indicado."""
    data_url = f"{BASE_URL}/{endpoint}"
    response = requests.get(
        data_url,
        headers={"x-apisports-key": API_KEY},
        params=params,
        timeout=30
    )
    response.raise_for_status()
    return response.json()["response"]


@task(task_run_name="transform_data")
def transform_data(data: list) -> pd.DataFrame:
    """Transforma los datos extraídos en un DataFrame plano y aplica limpieza inicial."""
    df = pd.json_normalize(data)
    df = rename_fixture_id(df)
    return df


@task(task_run_name="load_data-{endpoint_name}")
def load_data(df: pd.DataFrame, endpoint_name: str):
    """Guarda los datos transformados en la capa Bronze del datalake (particionado por event_date)."""
    scheduled_run = flow_run.scheduled_start_time.strftime("%Y-%m-%dT%H:%M")

    # Normalización mínima
    df = normalize_score_cols_to_float(df)
    df = add_event_date_from_fixture_date(df)

    # Directorio Bronze específico para el endpoint
    bronze_path = f"{DATALAKE_ROOT}/bronze/api_football/{endpoint_name}"
    os.makedirs(bronze_path, exist_ok=True)

    # Guardado en Bronze usando MERGE por fixture_id y partición por event_date
    save_new_data_as_delta(
        df,
        bronze_path,
        predicate="target.fixture_id = source.fixture_id",
        partition_cols=["event_date"],
    )

    print(f"Datos de {endpoint_name} guardados en Bronze ({scheduled_run})")


@task(task_run_name="to_silver-{endpoint_name}")
def transform_to_silver(endpoint_name: str) -> pd.DataFrame:
    """Lee la partición más reciente de Bronze, aplica transformaciones de Silver y guarda."""
    df_bronze_persisted = read_most_recent_partition(BRONZE_FIXTURES)

    df_silver = df_bronze_persisted.copy()
    df_silver = standardize_column_names(df_silver)
    df_silver = format_datetime_columns(df_silver)
    df_silver = drop_irrelevant_cols(df_silver)
    df_silver = add_match_winner(df_silver)
    df_silver = add_total_goals(df_silver)
    df_silver = cast_column_types(df_silver)

    df_silver = add_event_date_from_fixture_date(df_silver)
    df_silver["event_date"] = pd.to_datetime(df_silver["event_date"]).dt.strftime("%Y-%m-%d")

    os.makedirs(SILVER_FIXTURES, exist_ok=True)
    save_new_data_as_delta(
        df_silver,
        SILVER_FIXTURES,
        predicate="target.fixture_id = source.fixture_id",
        partition_cols=["event_date"],
    )
    return df_silver


@task(task_run_name="to_gold-{endpoint_name}")
def transform_to_gold_from_silver(endpoint_name: str) -> pd.DataFrame:
    """Lee todas las particiones de Silver, normaliza tipos y persiste subset en Gold + exporta."""
    df_silver_all = read_all_from_delta(SILVER_FIXTURES).copy()

    df_silver_all = cast_column_types(df_silver_all)
    df_silver_all = cast_gold_categoricals(df_silver_all)
    df_silver_all = ensure_event_date_utc(df_silver_all)

    df_to_save = df_silver_all[[
        "fixture_id", "event_date", "league_id", "league_name",
        "teams_home_name", "teams_away_name",
        "goals_home", "goals_away", "match_winner",
    ]].copy()

    df_to_save["event_date"] = pd.to_datetime(df_to_save["event_date"]).dt.strftime("%Y-%m-%d")

    os.makedirs(GOLD_FIXTURES, exist_ok=True)
    os.makedirs(EXPORTS_DIR, exist_ok=True)

    save_new_data_as_delta(
        df_to_save,
        GOLD_FIXTURES,
        predicate="target.fixture_id = source.fixture_id",
        partition_cols=["event_date"],
    )

    df_to_save.to_csv(f"{EXPORTS_DIR}/{endpoint_name}_gold.csv", index=False)
    df_to_save.to_parquet(f"{EXPORTS_DIR}/{endpoint_name}_gold.parquet", index=False)

    return df_silver_all


# -------------------- Flow --------------------

@flow
def etl_parametrizable(endpoints: list):
    for endpoint in endpoints:
        params = None
        if endpoint == "fixtures":
            fecha_ayer = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
            params = {"date": fecha_ayer, "timezone": "UTC"}

        data = extract_data(endpoint, params=params)
        df_bronze = transform_data(data)

        load_data(df_bronze, endpoint)
        transform_to_silver(endpoint)
        transform_to_gold_from_silver(endpoint)


if __name__ == "__main__":
    etl_parametrizable(endpoints=["fixtures"])
    # etl_parametrizable.serve(name="ETL-Fixtures", endpoints=["fixtures"])  # opcional