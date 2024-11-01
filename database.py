import pandas as pd
import os
import sqlite3


diretorio_csv = 'lei/'


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


conn = sqlite3.connect('projetos.db')


cursor = conn.cursor()


create_table_query = f'''
CREATE TABLE IF NOT EXISTS projetos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    {', '.join(f"{col} TEXT" for col in colunas)}
)
'''
cursor.execute(create_table_query)


for arquivo in os.listdir(diretorio_csv):
    if arquivo.endswith('.csv'):
      
        caminho_arquivo = os.path.join(diretorio_csv, arquivo)
        
        
        df_temp = pd.read_csv(caminho_arquivo, sep=',', encoding='utf-8', usecols=colunas)
        
        
        df_temp.to_sql('projetos', conn, if_exists='append', index=False)


conn.commit()
conn.close()

print("Tabela criada e arquivos CSV adicionados ao banco de dados SQLite com sucesso!")
