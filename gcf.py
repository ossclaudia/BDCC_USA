import functions_framework
from flask import jsonify
from google.cloud import bigquery, datastore
import datetime
import os

bq_client = bigquery.Client()
datastore_client = datastore.Client()

PROJECT_ID = "projetousa"
DATASET_ID = "MIMIC"
TABLE_REF = f"{PROJECT_ID}.{DATASET_ID}"
TABLE_ADMISSIONS = "ADMISSIONS"

####LAG(DISCHTIME) OVER (PARTITION BY SUBJECT_ID ORDER BY ADMITTIME) AS PREV_DISCHTIME - calcular a última alta hospitalar (DISCHTIME) antes de uma nova admissão (ADMITTIME)

@functions_framework.http  
def update_statistics(request):
    query = f"""
    WITH previous_admissions AS (
        SELECT 
            SUBJECT_ID, 
            HADM_ID, 
            ADMITTIME, 
            LAG(DISCHTIME) OVER (PARTITION BY SUBJECT_ID ORDER BY ADMITTIME) AS PREV_DISCHTIME 
        FROM `{TABLE_REF}.{TABLE_ADMISSIONS}`
    )
    SELECT 
        AVG(TIMESTAMP_DIFF(ADMITTIME, COALESCE(PREV_DISCHTIME, ADMITTIME), HOUR)) AS avg_waiting_hours,
        MAX(TIMESTAMP_DIFF(ADMITTIME, COALESCE(PREV_DISCHTIME, ADMITTIME), HOUR)) AS max_waiting_hours,
        COUNT(*) AS total_admissions
    FROM previous_admissions;
    """

    query_job = bq_client.query(query)
    results = query_job.result()

    statistics = []
    for row in results:
        stats = {
            "avg_waiting_hours": row.avg_waiting_hours,
            "max_waiting_hours": row.max_waiting_hours,
            "total_admissions": row.total_admissions,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }

        print(f"📊 Estatísticas calculadas: {stats}")  # DEBUG
        store_statistics(stats)
        statistics.append(stats)

    return jsonify({"status": "success", "statistics": statistics}), 200

'''def store_statistics(stats):
    entity = datastore.Entity(key=datastore_client.key("statistics"))
    entity.update(stats)
    print(stats)
    datastore_client.put(entity)'''


def store_statistics(stats):

    try:
        entity = datastore.Entity(key=datastore_client.key("statistics"))
        entity.update(stats)
        datastore_client.put(entity)

        print(" Estatísticas guardadas com sucesso!")  
    except Exception as e:
        print("Erro ao guardar estatísticas no Datastore:", str(e))  