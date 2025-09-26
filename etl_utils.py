# Librerías estándar
from datetime import datetime, timedelta

# Librerías externas
import requests
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError


def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parámetros:
    base_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API al que se realizará la solicitud.
    params (dict): Parámetros de consulta para enviar con la solicitud.
    data_field (str): El nombre del campo en el JSON que contiene los datos.
    headers (dict): Encabezados para enviar con la solicitud.

    Retorna:
    dict: Los datos obtenidos de la API en formato JSON o None si ocurre un error.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(
            endpoint_url,
            params=params,
            headers=headers,
            timeout=30  # evita bloqueos si la API no responde
        )
        response.raise_for_status()  # lanza excepción en caso de error HTTP

        try:
            data = response.json()
            if data_field:
                data = data[data_field]
        except ValueError:
            print("El formato de respuesta no es el esperado (JSON inválido).")
            return None

        return data

    except requests.exceptions.RequestException as e:
        print(f"La petición ha fallado: {e}")
        return None
    

def build_table(json_data):
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON.

    Parámetros:
    json_data (dict): Los datos en formato JSON obtenidos de una API.

    Retorna:
    DataFrame: Un DataFrame de pandas que contiene los datos.
    """
    try:
        df = pd.json_normalize(json_data)
        return df
    except:
        print("Los datos no están en el formato esperado")
        return None

def save_data_as_delta(df, path, mode="overwrite", partition_cols=None):
    """
    Guarda un dataframe en formato Delta Lake en la ruta especificada.
    A su vez, es capaz de particionar el dataframe por una o varias columnas.
    Por defecto, el modo de guardado es "overwrite".

    Args:
      df (pd.DataFrame): El dataframe a guardar.
      path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      mode (str): El modo de guardado. Son los modos que soporta la libreria
      deltalake: "overwrite", "append", "error", "ignore".
      partition_cols (list or str): La/s columna/s por las que se particionará el
      dataframe. Si no se especifica, no se particionará.
    """
    write_deltalake(
        path, df, mode=mode, partition_by=partition_cols
    )


def save_new_data_as_delta(new_data, data_path, predicate, partition_cols=None):
    """
    Guarda solo nuevos datos en formato Delta Lake usando la operación MERGE,
    comparando los datos ya cargados con los datos que se desean almacenar
    asegurando que no se guarden registros duplicados.

    Args:
      new_data (pd.DataFrame): Los datos que se desean guardar.
      data_path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      predicate (str): La condición de predicado para la operación MERGE.
    """

    try:
      dt = DeltaTable(data_path)
      new_data_pa = pa.Table.from_pandas(new_data)
      # Se insertan en target, datos de source que no existen en target
      dt.merge(
          source=new_data_pa,
          source_alias="source",
          target_alias="target",
          predicate=predicate
      ) \
      .when_not_matched_insert_all() \
      .execute()

    # Si no existe la tabla Delta Lake, se guarda como nueva
    except TableNotFoundError:
      save_data_as_delta(new_data, data_path, partition_cols=partition_cols)


def upsert_data_as_delta(data, data_path, predicate):
    """
    Guardar datos en formato Delta Lake usando la operacion MERGE.
    Cuando no haya registros coincidentes, se insertarán nuevos registros.
    Cuando haya registros coincidentes, se actualizarán los campos.

    Args:
      data (pd.DataFrame): Los datos que se desean guardar.
      data_path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      predicate (str): La condición de predicado para la operación MERGE.
    """
    try:
        dt = DeltaTable(data_path)
        data_pa = pa.Table.from_pandas(data)
        dt.merge(
            source=data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ) \
        .when_matched_update_all() \
        .when_not_matched_insert_all() \
        .execute()
    except TableNotFoundError:
        save_data_as_delta(data, data_path)


def read_most_recent_partition(data_path):
    """
    Lee la partición más reciente de una tabla Delta Lake,
    suponiendo que está particionada sólo por día (event_date).
    """
    try:
        requested_date = datetime.utcnow() - timedelta(days=1)  #  un día en lugar de una hora
        dt = DeltaTable(data_path)
        df_recent = dt.to_pandas(
            partitions=[
                ("event_date", "=", requested_date.strftime("%Y-%m-%d"))  # se usa la columna event_date
                # ("hora", "=", requested_date.strftime("%H"))  # no aplica, dado que se efectúa una partición por día
            ]
        )
        return df_recent
    except Exception as e:
        raise Exception(f"No se pudo procesar la tabla Delta Lake, por {e}")
    

def read_all_from_delta(path):
    """
    Lee todas las particiones de una tabla Delta Lake
    y devuelve un DataFrame de Pandas.
    """
    return DeltaTable(path).to_pandas()


def rename_fixture_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra 'fixture.id' a 'fixture_id' para evitar problemas con el punto en el nombre.
    Esto permite usar la columna de forma más clara y consistente en MERGE y consultas.
    """
    if "fixture.id" in df.columns:
        df = df.rename(columns={"fixture.id": "fixture_id"})
    return df


def normalize_score_cols_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte a float las columnas de score que pueden venir vacías desde la API.
    Esto evita que Spark infiera NullType y garantiza consistencia en el guardado en Bronze.
    """
    cols_to_float = [
        "score.extratime.home",
        "score.extratime.away",
        "score.penalty.home",
        "score.penalty.away",
    ]
    for col in cols_to_float:
        if col in df.columns:
            df[col] = df[col].astype("float")
    return df


def add_event_date_from_fixture_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la columna 'event_date' a partir de la fecha del fixture.
    
    - Si existe 'fixture.date' (nombre original de la API), se convierte a datetime 
      y se extrae solo la fecha (AAAA-MM-DD).
    - Si existe 'fixture_date' (tras estandarizar nombres en Silver), se aplica el mismo procedimiento.
    
    Esto asegura que la columna 'event_date' siempre esté presente y en un formato 
    consistente para su uso en particiones.
    """
    if "fixture.date" in df.columns:
        df["fixture.date"] = pd.to_datetime(df["fixture.date"])
        df["event_date"] = df["fixture.date"].dt.date
    elif "fixture_date" in df.columns:
        df["fixture_date"] = pd.to_datetime(df["fixture_date"])
        df["event_date"] = df["fixture_date"].dt.date
    return df


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza los nombres de columnas:
    - minúsculas
    - reemplaza puntos por guiones bajos ('.' -> '_')
    """
    df = df.rename(columns=lambda c: c.lower().replace(".", "_"))
    return df


def format_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte columnas de fecha/hora a formato datetime UTC con resolución en minutos.
    - fixture_date (string UTC -> datetime)
    - fixture_periods_first (UNIX -> datetime)
    - fixture_periods_second (UNIX -> datetime)
    """
    if "fixture_date" in df.columns:
        df["fixture_date"] = pd.to_datetime(df["fixture_date"], utc=True).dt.floor("min")
    if "fixture_periods_first" in df.columns:
        df["fixture_periods_first"] = pd.to_datetime(
            df["fixture_periods_first"], unit="s", utc=True
        ).dt.floor("min")
    if "fixture_periods_second" in df.columns:
        df["fixture_periods_second"] = pd.to_datetime(
            df["fixture_periods_second"], unit="s", utc=True
        ).dt.floor("min")
    return df


def drop_irrelevant_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina columnas sin impacto en el análisis (logos, flags, metadatos y duplicadas por derivación).
    Ajusta la lista según tus necesidades.
    """
    cols_drop = [
        "fixture_referee",
        "fixture_timezone",
        "fixture_timestamp",
        "fixture_periods_second",   # se mantiene 'first'; 'second' no aporta en Silver
        "fixture_status_long",
        "fixture_status_extra",
        "league_logo",
        "league_flag",
        "teams_home_logo",
        "teams_away_logo",
    ]
    existing = [c for c in cols_drop if c in df.columns]
    if existing:
        df = df.drop(columns=existing)
    return df


def add_match_winner(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea 'match_winner' a partir de 'goals_home' y 'goals_away':
    - 'Home' / 'Away' / 'Draw' según el marcador
    - pd.NA si faltan goles
    Luego elimina 'teams_home_winner' y 'teams_away_winner' si existen.
    """
    if "goals_home" in df.columns and "goals_away" in df.columns:
        def _winner(row):
            gh, ga = row["goals_home"], row["goals_away"]
            if pd.notna(gh) and pd.notna(ga):
                if gh > ga:  return "Home"
                if gh < ga:  return "Away"
                return "Draw"
            return pd.NA

        df["match_winner"] = df.apply(_winner, axis=1)

    drop_cols = [c for c in ["teams_home_winner", "teams_away_winner"] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


def add_total_goals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea 'total_goals' = goals_home + goals_away (maneja nulos).
    """
    if "goals_home" in df.columns and "goals_away" in df.columns:
        df["total_goals"] = df["goals_home"] + df["goals_away"]
    return df


def cast_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Castea columnas numéricas a Int64 (nullable) y textos a string para la capa Silver.
    Solo castea las columnas que existan en el DataFrame.
    """
    type_mapping_numeric = {
        "fixture_venue_id": "Int64",
        "fixture_status_elapsed": "Int64",
        "goals_home": "Int64",
        "goals_away": "Int64",
        "score_halftime_home": "Int64",
        "score_halftime_away": "Int64",
        "score_fulltime_home": "Int64",
        "score_fulltime_away": "Int64",
        "score_extratime_home": "Int64",
        "score_extratime_away": "Int64",
        "score_penalty_home": "Int64",
        "score_penalty_away": "Int64",
        "total_goals": "Int64",
    }
    type_mapping_string = {
        "fixture_venue_name": "string",
        "fixture_venue_city": "string",
        "fixture_status_short": "string",
        "league_name": "string",
        "league_country": "string",
        "league_round": "string",
        "teams_home_name": "string",
        "teams_away_name": "string",
        "match_winner": "string",
    }

    # Casteo seguro (solo columnas presentes)
    num_exists   = {k: v for k, v in type_mapping_numeric.items() if k in df.columns}
    text_exists  = {k: v for k, v in type_mapping_string.items() if k in df.columns}

    if num_exists:
        df = df.astype(num_exists)
    if text_exists:
        df = df.astype(text_exists)

    return df


def cast_gold_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte a 'category' columnas de baja cardinalidad para optimizar memoria y agrupaciones.
    Solo castea columnas existentes.
    """
    categoricals = {
        "fixture_status_short": "category",
        "league_round": "category",
        "match_winner": "category",
    }
    existing = {k: v for k, v in categoricals.items() if k in df.columns}
    if existing:
        df = df.astype(existing)
    return df


def ensure_event_date_utc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Asegura 'event_date' como datetime con zona horaria UTC (no string).
    Útil para filtros y cálculos temporales consistentes en memoria (Gold).
    """
    if "event_date" in df.columns:
        df["event_date"] = pd.to_datetime(df["event_date"], utc=True)
    return df