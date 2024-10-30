import os
import inspect
import requests
import csv

csv.field_size_limit(5**20)

CURRENTDIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
offset = 0

def truncar_campos(campos):
    return [x[:131072].strip() if isinstance(x, str) else x for x in campos]

while True:
    try:
        path_name = f"{CURRENTDIR}/lei/{offset}.csv"
        if os.path.exists(path_name):
            print(f"Arquivo {path_name} já existe. Pulando...")
            offset += 1
            continue

        print(f"[+] Offset: {offset}", end=" ")
        print(" OK ")

        CSV_URL = f'https://api.salic.cultura.gov.br/v1/projetos/?sort=PRONAC:asc&offset={offset}&limit=1&format=csv'
        with requests.Session() as s:
            download = s.get(CSV_URL)
            if download.status_code == 200:
                decoded_content = download.content.decode('utf-8')
                cr = csv.reader(decoded_content.splitlines(), delimiter=',')
                my_list = list(cr)

                if len(my_list) > 1:
                 
                    campos_truncados = truncar_campos(my_list[1])

                    
                    if len(campos_truncados) == 32:
                        
                        with open(path_name, "w", encoding='utf-8') as f:
                            f.write(decoded_content)
                    else:
                        print(f"Erro: Esperado 32 campos, mas foram encontrados {len(campos_truncados)}.")
                else:
                    print(f"Nenhum registro encontrado para o offset {offset}.")
                    break
            else:
                print(f"Falha no Download: Status code {download.status_code}")
                print(f"Pulando para o próximo arquivo...")

        offset += 1

    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        print(f"Pulando para o próximo arquivo...")
