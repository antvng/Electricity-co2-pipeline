from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def fetch_realtime_regional_and_upsert() -> None:
    """
    Récupère les 100 dernières mesures temps réel régionales RTE
    et fait un MERGE INTO dans Snowflake.

    """
    import requests
    import snowflake.connector

    url = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
        "eco2mix-regional-tr/records"
    )
    params = {
        "limit": 100,
        "order_by": "date_heure DESC",
        "timezone": "Europe/Paris",
    }

    logger.info("Récupération des données régionales temps réel RTE...")
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    records = response.json().get("results", [])
    logger.info(f"{len(records)} mesures régionales récupérées depuis l'API.")

    if not records:
        logger.warning("Aucune donnée retournée par l'API temps réel régionale.")
        return

    conn = snowflake.connector.connect(
        user=Variable.get("SNOW_USER"),
        password=Variable.get("SNOW_PASSWORD"),
        account=Variable.get("SNOW_ACCOUNT"),
        warehouse=Variable.get("SNOW_WAREHOUSE"),
        database=Variable.get("SNOW_DATABASE"),
        schema="ECO2MIX",
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW_REGIONAL_TEMPS_REEL (
            code_insee_region   VARCHAR(10),
            libelle_region      VARCHAR(100),
            date_heure          TIMESTAMP_TZ,
            consommation        FLOAT,
            thermique           FLOAT,
            nucleaire           FLOAT,
            eolien              FLOAT,
            solaire             FLOAT,
            hydraulique         FLOAT,
            pompage             FLOAT,
            bioenergies         FLOAT,
            ech_physiques       FLOAT,
            tco_thermique       FLOAT,
            tco_nucleaire       FLOAT,
            tco_eolien          FLOAT,
            tco_solaire         FLOAT,
            tco_hydraulique     FLOAT,
            tco_bioenergies     FLOAT,
            _loaded_at          TIMESTAMP_NTZ,
            _source             VARCHAR(50)
        )
    """)

    merge_sql = """
        MERGE INTO RAW_REGIONAL_TEMPS_REEL t
        USING (
            SELECT
                %(code_insee_region)s           AS code_insee_region,
                %(libelle_region)s              AS libelle_region,
                %(date_heure)s::TIMESTAMP_TZ    AS date_heure,
                %(consommation)s                AS consommation,
                %(thermique)s                   AS thermique,
                %(nucleaire)s                   AS nucleaire,
                %(eolien)s                      AS eolien,
                %(solaire)s                     AS solaire,
                %(hydraulique)s                 AS hydraulique,
                %(pompage)s                     AS pompage,
                %(bioenergies)s                 AS bioenergies,
                %(ech_physiques)s               AS ech_physiques,
                %(tco_thermique)s               AS tco_thermique,
                %(tco_nucleaire)s               AS tco_nucleaire,
                %(tco_eolien)s                  AS tco_eolien,
                %(tco_solaire)s                 AS tco_solaire,
                %(tco_hydraulique)s             AS tco_hydraulique,
                %(tco_bioenergies)s             AS tco_bioenergies
        ) s
        ON t.date_heure = s.date_heure
        AND t.code_insee_region = s.code_insee_region
        WHEN MATCHED AND t.consommation != s.consommation THEN UPDATE SET
            t.consommation  = s.consommation,
            t.thermique     = s.thermique,
            t.nucleaire     = s.nucleaire,
            t.eolien        = s.eolien,
            t._loaded_at    = %(loaded_at)s
        WHEN NOT MATCHED THEN INSERT (
            code_insee_region, libelle_region, date_heure,
            consommation, thermique, nucleaire, eolien, solaire,
            hydraulique, pompage, bioenergies, ech_physiques,
            tco_thermique, tco_nucleaire, tco_eolien, tco_solaire,
            tco_hydraulique, tco_bioenergies,
            _loaded_at, _source
        ) VALUES (
            s.code_insee_region, s.libelle_region, s.date_heure,
            s.consommation, s.thermique, s.nucleaire, s.eolien, s.solaire,
            s.hydraulique, s.pompage, s.bioenergies, s.ech_physiques,
            s.tco_thermique, s.tco_nucleaire, s.tco_eolien, s.tco_solaire,
            s.tco_hydraulique, s.tco_bioenergies,
            %(loaded_at)s, 'eco2mix-regional-tr'
        )
    """

    loaded_at = datetime.utcnow()

    for record in records:
        params_row = {
            "code_insee_region": record.get("code_insee_region"),
            "libelle_region":    record.get("libelle_region"),
            "date_heure":        record.get("date_heure"),
            "consommation":      record.get("consommation"),
            "thermique":         record.get("thermique"),
            "nucleaire":         record.get("nucleaire"),
            "eolien":            record.get("eolien"),
            "solaire":           record.get("solaire"),
            "hydraulique":       record.get("hydraulique"),
            "pompage":           record.get("pompage"),
            "bioenergies":       record.get("bioenergies"),
            "ech_physiques":     record.get("ech_physiques"),
            "tco_thermique":     record.get("tco_thermique"),
            "tco_nucleaire":     record.get("tco_nucleaire"),
            "tco_eolien":        record.get("tco_eolien"),
            "tco_solaire":       record.get("tco_solaire"),
            "tco_hydraulique":   record.get("tco_hydraulique"),
            "tco_bioenergies":   record.get("tco_bioenergies"),
            "loaded_at":         loaded_at,
        }
        cursor.execute(merge_sql, params_row)

    conn.commit()
    conn.close()
    logger.info(f"MERGE INTO régional terminé — {len(records)} enregistrements traités.")


def verify_recent_regional_data() -> None:
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
    cursor.execute("""
        SELECT COUNT(*), MAX(date_heure)
        FROM RAW_REGIONAL_TEMPS_REEL
    """)
    row = cursor.fetchone()
    conn.close()

    logger.info(f"Total lignes régionales temps réel : {row[0]:,}")
    logger.info(f"Dernière mesure : {row[1]}")


with DAG(
    dag_id="incremental_horaire_regional",
    description="Récupère les données régionales RTE temps réel toutes les heures via MERGE INTO",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["incremental", "bronze", "regional", "hourly"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_and_upsert_regional",
        python_callable=fetch_realtime_regional_and_upsert,
    )

    verify = PythonOperator(
        task_id="verify_recent_regional_data",
        python_callable=verify_recent_regional_data,
    )

    fetch >> verify