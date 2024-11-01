import pandas as pd
import os
import sqlite3
import re

# Diretório onde os arquivos CSV estão localizados
diretorio_csv = 'lei/'  # Substitua pelo seu diretório

# Colunas do CSV, exceto 'id'
colunas = [
    "etapa", "providencia", "area", "enquadramento", "objetivos",
    "ficha_tecnica", "situacao", "outras_fontes", "acessibilidade",
    "sinopse", "nome", "cgccpf", "mecanismo", "segmento",
    "PRONAC", "estrategia_execucao", "valor_aprovado", "justificativa",
    "resumo", "valor_solicitado", "especificacao_tecnica", "municipio",
    "data_termino", "UF", "impacto_ambiental", "democratizacao",
    "valor_projeto", "proponente", "ano_projeto", "data_inicio",
    "valor_captado", "valor_proposta"
]

# Conecta ao banco de dados SQLite (ou cria um novo)
conn = sqlite3.connect('projetos.db')

# Cria a tabela se não existir, incluindo a chave primária 'id'
with conn:
    conn.execute(f'''
    CREATE TABLE IF NOT EXISTS projetos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join(f"{col} TEXT" for col in colunas)}
    )
    ''')

# Função para extrair o número do nome do arquivo
def extrair_numero(nome_arquivo):
    numero = re.findall(r'\d+', nome_arquivo)
    return int(numero[0]) if numero else float('inf')

# Obtém e ordena os arquivos CSV em ordem numérica com base no número extraído do nome do arquivo
arquivos_csv = sorted(
    [f for f in os.listdir(diretorio_csv) if f.endswith('.csv')],
    key=extrair_numero
)

# Lista para armazenar dados de cada arquivo temporariamente
data_to_insert = []

# Processa cada arquivo CSV em ordem crescente de nome numérico
for arquivo in arquivos_csv:
    caminho_arquivo = os.path.join(diretorio_csv, arquivo)
    
    try:
        # Lê o arquivo CSV, carregando somente as colunas especificadas
        df_temp = pd.read_csv(caminho_arquivo, sep=',', encoding='utf-8', usecols=colunas)
        
        # Converte os dados do DataFrame para uma lista de dicionários e adiciona à lista principal
        data_to_insert.extend(df_temp.to_dict(orient='records'))
        print(f"Arquivo '{arquivo}' processado com sucesso.")

    except ValueError as ve:
        print(f"Erro de valor ao processar '{arquivo}': {ve}")
    except FileNotFoundError:
        print(f"Arquivo '{arquivo}' não encontrado.")
    except Exception as e:
        print(f"Erro ao processar '{arquivo}': {e}")

# Insere todos os dados no banco de dados em uma única operação
try:
    if data_to_insert:
        with conn:
            conn.executemany(f'''
            INSERT INTO projetos ({', '.join(colunas)}) 
            VALUES ({', '.join(['?' for _ in colunas])})
            ''', [tuple(d[col] for col in colunas) for d in data_to_insert])
        print("Dados inseridos com sucesso no banco de dados.")
    else:
        print("Nenhum dado para inserir.")

except sqlite3.DatabaseError as db_err:
    print(f"Erro ao inserir dados no banco de dados: {db_err}")

# Fecha a conexão com o banco de dados
conn.close()
print("Conexão com o banco de dados fechada.")
