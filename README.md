# ⚽ ETL API Football

Este proyecto implementa un pipeline **ETL (Extract, Transform, Load)** utilizando la **API-FOOTBALL v3** y almacenamiento en **Delta Lake**.  
El objetivo es extraer datos de fútbol (ligas, países, fixtures, etc.), transformarlos y almacenarlos siguiendo una arquitectura de datos en capas: **Bronze → Silver → Gold**.

---

## 🚀 Tecnologías utilizadas
- Python 3.x  
- Pandas  
- Requests  
- PyArrow  
- Delta Lake (`deltalake`)  
- Jupyter / VS Code  

---

## 📂 Estructura del proyecto

ETL_api_football/
│── ETL_API_Football.ipynb # Notebook principal con el pipeline
│── etl_utils.py # Funciones auxiliares (ETL helpers)
│── requirements.txt # Librerías necesarias
│── pipeline.conf # Archivo de configuración (API key, parámetros, etc.)
│── datalake/ # Carpetas de almacenamiento en Delta Lake
│ ├── bronze/ # Datos crudos (tal como vienen de la API)
│ ├── silver/ # Datos transformados y normalizados
│ └── gold/ # Datos listos para análisis y visualización

---

## ⚙️ Configuración
1. Crear un entorno virtual con Anaconda o venv.  
2. Instalar las dependencias:  
   ```bash
   pip install -r requirements.txt
Configurar la API Key en el archivo pipeline.conf.

---

🏗️ Flujo ETL

1. Extracción: se obtienen datos de la API-FOOTBALL v3.

2. Transformación: se normalizan, limpian y enriquecen los datos.

3. Carga: se almacenan en Delta Lake en tres capas:

    -Bronze: datos crudos.

    -Silver: datos transformados.

    -Gold: datos listos para análisis y visualización.

---

📊 Resultados esperados
-Acumulación de fixtures históricos por fecha.

-Tablas de ligas, países y equipos limpias y normalizadas.

-Posibilidad de crear dashboards o notebooks de análisis sobre la capa Gold.

✍️ Autor: Elias Fernández
📧 Contacto: fernandezelias86@gmail.com