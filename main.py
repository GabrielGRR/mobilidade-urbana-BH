import urllib.request
import urllib.error
import json
from pathlib import Path
from datetime import datetime

def extract_events():
    try:
        url = 'https://temporeal.pbh.gov.br/?param=D'  
        
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            
            output_dir = Path("data/bronze")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            output_file = output_dir / f"events_{timestamp}.json"
            
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                
            print(f"Sucesso! {len(data)} registros de ônibus em tempo real salvos em {output_file}")
            
    except urllib.error.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except urllib.error.URLError as err:
        print(f"Connection error occurred: {err}")
    except Exception as err:
        print(f"Error occurred: {err}")


def main():
    extract_events()

if __name__ == "__main__":
    main()
