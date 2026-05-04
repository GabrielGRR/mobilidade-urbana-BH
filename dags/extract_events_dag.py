from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone, timedelta
import urllib.request
import json
import time
from pathlib import Path
from utils.logging_utils import setup_logging, get_logger
from utils.decorators import retry

logger = get_logger(__name__)

@retry(max_attempts=3, delay=2)
def extract_events():
    url = 'https://temporeal.pbh.gov.br/?param=D'  
    
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    with urllib.request.urlopen(req, timeout=10) as response:
        data = json.loads(response.read().decode('utf-8'))
        
        # Caminho absoluto dentro do container do Airflow
        output_dir = Path("/opt/airflow/data/bronze")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        today = datetime.now().strftime("%Y-%m-%d")
        output_file = output_dir / f"events_{today}.jsonl"
        
        existing_keys = set()
        
        if output_file.exists():
            with open(output_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        record = json.loads(line)
                        existing_keys.add((record.get("HR"), record.get("NV")))
        
        new_records = 0
        
        with open(output_file, "a", encoding="utf-8") as f:
            for item in data:
                key = (item.get("HR"), item.get("NV"))
                if key not in existing_keys:
                    item["ingested_at"] = datetime.now(timezone.utc).isoformat()
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
                    existing_keys.add(key)
                    new_records += 1
            
        logger.info(f"Sucesso! {new_records} novos registros adicionados.")

def poll_api_for_15_minutes():
    setup_logging()
    window_minutes = 15
    end_time = datetime.now() + timedelta(minutes=window_minutes)
    
    logger.info(f"Iniciando ciclo de captura no Airflow. Script rodará até {end_time.strftime('%H:%M:%S')}")
    
    while datetime.now() < end_time:
        cycle_start = time.time()
        
        try:
            extract_events()
        except Exception as e:
            logger.exception("Falha na extração. Tentando novamente em 20s...")
        
        elapsed = time.time() - cycle_start
        sleep_time = max(0, 20 - elapsed)
        time.sleep(sleep_time)
        
    logger.info("Ciclo finalizado. Task do Airflow concluída com sucesso.")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'ingestion_pbh_events_bronze',
    default_args=default_args,
    description='Extrai posições de ônibus da PBH a cada 15 minutos e salva na camada Bronze',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 5, 1),
    catchup=False, # Não rodar o passado retroativamente
    tags=['ingestion', 'bronze', 'mobility'],
) as dag:

    extract_task = PythonOperator(
        task_id='poll_api_15_min',
        python_callable=poll_api_for_15_minutes,
    )
