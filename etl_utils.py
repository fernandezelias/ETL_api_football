# Librerías estándar
from datetime import datetime, timedelta

# Librerías externas
import requests
import pandas as pd
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
    dict: Los datos obtenidos de la API en formato JSON.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            if data_field:
              data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
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