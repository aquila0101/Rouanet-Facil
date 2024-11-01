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

# Processa e insere cada arquivo CSV individualmente no banco de dados
for arquivo in arquivos_csv:
    caminho_arquivo = os.path.join(diretorio_csv, arquivo)
    
    try:
        # Lê o arquivo CSV, carregando somente as colunas especificadas
        df_temp = pd.read_csv(caminho_arquivo, sep=',', encoding='utf-8', usecols=colunas)
        
        # Converte os dados do DataFrame para uma lista de tuplas para inserção
        registros = [tuple(row[col] for col in colunas) for _, row in df_temp.iterrows()]
        
        # Insere os dados do arquivo atual no banco de dados como uma operação independente
        with conn:
            conn.executemany(f'''
            INSERT INTO projetos ({', '.join(colunas)}) 
            VALUES ({', '.join(['?' for _ in colunas])})
            ''', registros)
        
        print(f"Arquivo '{arquivo}' processado e inserido com sucesso.")

    except ValueError as ve:
        print(f"Erro de valor ao processar '{arquivo}': {ve}")
    except FileNotFoundError:
        print(f"Arquivo '{arquivo}' não encontrado.")
    except Exception as e:
        print(f"Erro ao processar '{arquivo}': {e}")

# Fecha a conexão com o banco de dados
conn.close()
print("Conexão com o banco de dados fechada.")
