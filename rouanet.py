import os
import sys
import inspect
import requests
import csv

# Limite de tamanho dos campos para o CSV
csv.field_size_limit(5**20)

CURRENTDIR = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
offset = 0
error_count = 0  # Contador de erros consecutivos
max_errors = 5   # Limite de erros consecutivos permitidos antes de encerrar

def truncar_campos(campos):
    """Trunca campos para um limite de 131072 caracteres, caso seja uma string."""
    return [x[:131072].strip() if isinstance(x, str) else x for x in campos]

while True:
    try:
        path_name = f"{CURRENTDIR}/lei/{offset}.csv"
        if os.path.exists(path_name):
            print(f"Arquivo {path_name} já existe. Pulando...")
            offset += 1
            continue

        print(f"[+] Offset: {offset}", end=" ")
        
        CSV_URL = f'https://api.salic.cultura.gov.br/v1/projetos/?sort=PRONAC:asc&offset={offset}&limit=1&format=csv'
        with requests.Session() as s:
            try:
                download = s.get(CSV_URL, timeout=10)
                download.raise_for_status()  # Levanta erro em caso de status != 200
            except requests.exceptions.RequestException as e:
                print(f"Falha no download: {e}")
                error_count += 1
                if error_count >= max_errors:
                    print("Servidor parece estar offline após múltiplas tentativas. Encerrando.")
                    sys.exit(1)
                print(f"Pulando para o próximo offset... ({error_count}/{max_errors} tentativas falhas)")
                offset += 1
                continue
            else:
                # Reseta o contador de erros após um download bem-sucedido
                error_count = 0

            try:
                decoded_content = download.content.decode('utf-8')
            except UnicodeDecodeError:
                print("Erro de decodificação do conteúdo.")
                print("Pulando para o próximo offset...")
                offset += 1
                continue

            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)

            if len(my_list) > 1:  # Certifique-se de que há mais de uma linha (cabeçalho + dados)
                campos_truncados = truncar_campos(my_list[1])

                if len(campos_truncados) == 32:
                    try:
                        with open(path_name, "w", encoding='utf-8') as f:
                            writer = csv.writer(f)
                            writer.writerow(my_list[0])  # Cabeçalho
                            writer.writerow(campos_truncados)  # Dados truncados
                        print("Arquivo salvo com sucesso.")
                    except IOError as e:
                        print(f"Erro ao salvar o arquivo {path_name}: {e}")
                else:
                    print(f"Erro: Esperado 32 campos, mas foram encontrados {len(campos_truncados)}.")
            else:
                print(f"Nenhum registro encontrado para o offset {offset}.")
                break

        offset += 1

    except KeyboardInterrupt:
        print("Execução interrompida pelo usuário.")
        sys.exit(0)
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        print("Pulando para o próximo offset...")
        offset += 1
