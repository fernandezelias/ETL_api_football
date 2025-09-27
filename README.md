🌐 Available in: [Español](README.md) | [English](README_EN.md)

# ⚽ ETL API Football

Proyecto de **Data Engineering** que construye un pipeline ETL utilizando la **API-FOOTBALL v3** y almacenamiento en **Delta Lake**.  
El objetivo es extraer datos de *fixtures*, ligas y equipos, transformarlos y organizarlos en capas **Bronze → Silver → Gold**, con exportación a **CSV y Parquet** para análisis y visualización.

---

## 🚀 Tecnologías
- Python (Pandas, Requests, PyArrow)  
- Delta Lake (`deltalake`)  
- Prefect (orquestación)  
- Jupyter / VS Code  
- Seaborn y Matplotlib (visualización)

---

## 📂 Estructura del repositorio
```plaintext
ETL_API_Football/
│── ETL_API_Football.ipynb          # Notebook manual (ejecución y guardados activos)
│── ETL_API_Football_Prefect.ipynb  # Notebook de orquestación con Prefect (demo y documentación)
│── scripts/
│   └── etl_fixtures.py             # Pipeline orquestado (endpoint dinámico: fixtures)
│── etl_utils.py                    # Helpers (requests, transformaciones, Delta Lake)
│── requirements.txt                 # Dependencias
│── pipeline.conf                    # Configuración (API key, parámetros, rutas)
│── datalake/
│   ├── bronze/                     # Datos crudos
│   ├── silver/                     # Datos transformados
│   ├── gold/                       # Datos listos para análisis
│   └── exports/                    # Archivos exportados (CSV/Parquet)
```

---

## 🏗️ Flujo ETL
1. **Extracción** → requests a la API-FOOTBALL (en orquestación se prioriza `fixtures`, datos dinámicos).  
2. **Transformación** → normalización y limpieza.  
3. **Carga** → Delta Lake por capas:  
   - **Bronze**: datos crudos (con mínimas normalizaciones para persistencia).  
   - **Silver**: datos transformados y normalizados.  
   - **Gold**: datos curados, listos para análisis y exportación.  

> Nota: los endpoints **estáticos** (`countries`, `leagues`) se cargan de forma inicial y se actualizan de manera ocasional (fuera del flow diario de `fixtures`).  

---

## ▶️ Uso del flujo desde `scripts/etl_fixtures.py`

El flujo ETL está definido en `scripts/etl_fixtures.py`.  
En este repositorio es posible:

- **Opción A — Corrida única (demo one-off):** ejecuta el flow una sola vez para validar la orquestación.  
- **Opción B — Servir el flow (opcional):** mantiene el flow activo como servicio local.  

### Opción A — Ejecutar una corrida orquestada

En un notebook de Jupyter/VS Code:

```python
import importlib

# Importa el script de orquestación
etl = importlib.import_module("scripts.etl_fixtures")

# Ejecuta el flujo ETL de forma local (demo one-off)
etl.etl_parametrizable(endpoints=["fixtures"])
```

También es posible ejecutarlo desde la terminal (ej. Anaconda Prompt):

```bash
python scripts/etl_fixtures.py
```

O directamente dentro del Notebook:

```python
!python scripts/etl_fixtures.py
```

Todas estas opciones llaman al mismo flow definido en `scripts/etl_fixtures.py`.

---

## 📊 Resultados esperados
- Acumulación de **fixtures históricos** con ingesta incremental.  
- Tablas limpias de **ligas, copas, países, equipos y partidos**.  
- Capa Gold lista para dashboards y análisis exploratorio.

---

📄 Licencia
Este proyecto está bajo la licencia MIT.

---

✍️ Autor: Elias Fernández  
📧 Contacto: fernandezelias86@gmail.com  
🔗 LinkedIn: www.linkedin.com/in/eliasfernandez208