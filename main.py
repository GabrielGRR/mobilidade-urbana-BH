import urllib.request
import urllib.error
import json
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from utils.logging_utils import setup_logging, get_logger
from utils.decorators import retry

logger = get_logger(__name__)

@retry(max_attempts=3, delay=2)
def extract_events():
    url = 'https://temporeal.pbh.gov.br/?param=D'  
    
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    # Adicionando timeout de 10 segundos para blindar travamentos e permitir que o retry atue
    with urllib.request.urlopen(req, timeout=10) as response:
        data = json.loads(response.read().decode('utf-8'))
        
        output_dir = Path("data/bronze")
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

def main():
    setup_logging()
    
    # Define a janela de execução (15 minutos para rodar seguro no Airflow)
    window_minutes = 15
    end_time = datetime.now() + timedelta(minutes=window_minutes)
    
    logger.info(f"Iniciando ciclo de captura. O script rodará a cada 20s até {end_time.strftime('%H:%M:%S')}")
    
    while datetime.now() < end_time:
        cycle_start = time.time()
        
        try:
            extract_events()
        except Exception as e:
            # Se a API cair por completo (mesmo após os 3 retries), ele cai aqui.
            # O erro é reportado, mas não quebra o loop de 15 minutos do Airflow.
            logger.exception("Falha catastrófica no ciclo atual. Pulando para o próximo em 20s...")
        
        # Calcula quanto tempo o script levou e dorme apenas o resto para fechar exatos 20s
        elapsed = time.time() - cycle_start
        sleep_time = max(0, 20 - elapsed)
        time.sleep(sleep_time)
        
    logger.info("Ciclo de 15 minutos finalizado. O Airflow pode marcar a task como Sucesso.")

if __name__ == "__main__":
    main()
