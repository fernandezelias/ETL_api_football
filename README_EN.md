🌐 Available in: [Español](README.md) | [English](README_EN.md)

# ⚽ ETL API Football

**Data Engineering** project that builds an ETL pipeline using **API-FOOTBALL v3** and storage in **Delta Lake**.  
The goal is to extract *fixtures*, leagues, and teams data, transform and organize them into **Bronze → Silver → Gold** layers, with export to **CSV and Parquet** for analysis and visualization.

---

## 🚀 Technologies
- Python (Pandas, Requests, PyArrow)  
- Delta Lake (`deltalake`)  
- Prefect (orchestration)  
- Jupyter / VS Code  
- Seaborn and Matplotlib (visualization)

---

## 📂 Repository structure
```plaintext
ETL_API_Football/
│── ETL_API_Football.ipynb          # Manual notebook (active execution and saves)
│── ETL_API_Football_Prefect.ipynb  # Orchestration notebook with Prefect (demo and documentation)
│── scripts/
│   └── etl_fixtures.py             # Orchestrated pipeline (dynamic endpoint: fixtures)
│── etl_utils.py                    # Helpers (requests, transformations, Delta Lake)
│── requirements.txt                 # Dependencies
│── pipeline.conf                    # Configuration (API key, parameters, paths)
│── datalake/
│   ├── bronze/                     # Raw data
│   ├── silver/                     # Transformed data
│   ├── gold/                       # Data ready for analysis
│   └── exports/                    # Exported files (CSV/Parquet)
```

---

## 🏗️ ETL Flow
1. **Extraction** → requests to API-FOOTBALL (in orchestration, `fixtures` are prioritized as dynamic data).  
2. **Transformation** → normalization and cleaning.  
3. **Load** → Delta Lake by layers:  
   - **Bronze**: raw data (with minimal normalizations for persistence).  
   - **Silver**: transformed and normalized data.  
   - **Gold**: curated data, ready for analysis and export.  

> Note: **static endpoints** (`countries`, `leagues`) are loaded initially and updated occasionally (outside the daily `fixtures` flow).  

---

## ▶️ Using the flow from `scripts/etl_fixtures.py`

The ETL flow is defined in `scripts/etl_fixtures.py`.  
In this repository, you can:

- **Option A — One-off run (demo):** execute the flow once to validate orchestration.  
- **Option B — Serve the flow (optional):** keep the flow active as a local service.  

### Option A — Run an orchestrated flow

In a Jupyter/VS Code notebook:

```python
import importlib

# Import the orchestration script
etl = importlib.import_module("scripts.etl_fixtures")

# Run the ETL flow locally (one-off demo)
etl.etl_parametrizable(endpoints=["fixtures"])
```

It can also be run from the terminal (e.g., Anaconda Prompt):

```bash
python scripts/etl_fixtures.py
```

Or directly inside the Notebook:

```python
!python scripts/etl_fixtures.py
```

All these options call the same flow defined in `scripts/etl_fixtures.py`.

---

## 📊 Expected results
- Accumulation of **historical fixtures** with incremental ingestion.  
- Clean tables of leagues, cups, countries, teams, and matches.  
- Gold layer ready for dashboards and exploratory analysis.

---

📄 License
This project is under the MIT License.

---

✍️ Author: Elias Fernández  
📧 Contact: fernandezelias86@gmail.com  
🔗 LinkedIn: www.linkedin.com/in/eliasfernandez208
