from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def fetch_and_load_regional_year(year: int) -> None:
    import requests
    import pandas as pd
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    import io

    # API Call RTE — endpoint régional consolidé
    url = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
        "eco2mix-regional-cons-def/exports/csv"
    )
    params = {
        "limit": -1,
        "timezone": "Europe/Paris",
        "use_labels": False,
        "delimiter": ";",
        "where": (
            f"date_heure >= '{year}-01-01T00:00:00+01:00' "
            f"AND date_heure < '{year + 1}-01-01T00:00:00+01:00'"
        ),
    }

    logger.info(f"[{year}] Téléchargement des données régionales depuis l'API ODRÉ...")
    response = requests.get(url, params=params, timeout=600)
    response.raise_for_status()

    # Parsing du CSV
    df = pd.read_csv(io.StringIO(response.text), sep=";")

    if df.empty:
        logger.warning(f"[{year}] Aucune donnée retournée.")
        return

    logger.info(f"[{year}] {len(df)} lignes téléchargées.")

    # Nettoyage minimal des noms de colonnes (Bronze)
    df.columns = [
        col.lower()
           .replace(" ", "_")
           .replace("(", "")
           .replace(")", "")
           .replace("-", "_")
           .replace("é", "e")
           .replace("è", "e")
           .replace("ê", "e")
           .replace("à", "a")
           .replace("ô", "o")
        for col in df.columns
    ]

    # Colonne de traçabilité
    df["_loaded_at"] = datetime.utcnow()

    # Forcer les colonnes mixtes en string pour éviter ArrowTypeError
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str)

    # Remplacer les "nan" string créés par astype(str) sur des NaN
    df = df.replace("nan", None)

    logger.info(f"[{year}] Colonnes : {list(df.columns)}")

    # Connexion Snowflake
    conn = snowflake.connector.connect(
        user=Variable.get("SNOW_USER"),
        password=Variable.get("SNOW_PASSWORD"),
        account=Variable.get("SNOW_ACCOUNT"),
        warehouse=Variable.get("SNOW_WAREHOUSE"),
        database=Variable.get("SNOW_DATABASE"),
        schema="ECO2MIX",
    )

    # Chargement dans Snowflake
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="RAW_REGIONAL_HISTORIQUE",
        auto_create_table=True,
        overwrite=False,
    )

    conn.close()
    logger.info(f"[{year}] ✅ {nrows} lignes chargées dans Snowflake.")


def verify_regional_load() -> None:
    import snowflake.connector

    conn = snowflake.connector.connect(
        user=Variable.get("SNOW_USER"),
        password=Variable.get("SNOW_PASSWORD"),
        account=Variable.get("SNOW_ACCOUNT"),
        warehouse=Variable.get("SNOW_WAREHOUSE"),
        database=Variable.get("SNOW_DATABASE"),
        schema="ECO2MIX",
    )

    cursor = conn.cursor()
    cursor.execute(
        'SELECT COUNT(*), MIN("date_heure"), MAX("date_heure") '
        "FROM RAW_REGIONAL_HISTORIQUE"
    )
    row = cursor.fetchone()
    conn.close()

    logger.info("✅ CHECK FINALE RÉGIONAL :")
    logger.info(f"   Total lignes : {row[0]:,}")
    logger.info(f"   Date min     : {row[1]}")
    logger.info(f"   Date max     : {row[2]}")

    if row[0] < 1_000_000:
        raise ValueError(
            f"Seulement {row[0]} lignes chargées — quelque chose a mal tourné !"
        )


# Définition du DAG
with DAG(
    dag_id="backfill_historique_regional",
    description="Charge tout l'historique régional éco2mix (2013→2025) dans Snowflake Bronze",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["backfill", "bronze", "regional"],
) as dag:

    tasks = []
    for year in range(2013, 2026):
        task = PythonOperator(
            task_id=f"load_regional_year_{year}",
            python_callable=fetch_and_load_regional_year,
            op_kwargs={"year": year},
        )
        tasks.append(task)

    verify = PythonOperator(
        task_id="verify_regional_load",
        python_callable=verify_regional_load,
    )

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
    tasks[-1] >> verify