import pandas as pd
import psycopg2
from psycopg2 import sql
from decimal import Decimal

# -------------------------------------------------
# Função utilitária para checar se a coluna existe
# no DataFrame. Se não existir, retorna None.
# -------------------------------------------------
def safe_get_value(row, col_name, df):
    if col_name not in df.columns:
        return None
    return row[col_name]

# -------------------------------------------------
# Caminho do arquivo Excel
# -------------------------------------------------
excel_file = 'inserir_orcamento\Orçamento 25 Base.xlsx'

# -------------------------------------------------
# Ler dados do Excel
# -------------------------------------------------
df = pd.read_excel(excel_file)

# -------------------------------------------------
# Conversão da coluna DATA para formato de data padrão (caso necessário)
# -------------------------------------------------
if 'DATA' in df.columns:
    df['DATA'] = pd.to_datetime(df['DATA'], format='%d/%m/%Y', errors='coerce')

# -------------------------------------------------
# Definir nomes de colunas que desejamos no banco
# -------------------------------------------------
unidade_col = 'UNIDADE'
dia_col = 'DIA'
data_col = 'DATA'
cte_col = 'CTE'
valor_col = 'VALOR'
volume_col = 'VOLUME'
peso_col = 'PESO'

# -------------------------------------------------
# Dados de conexão ao Postgres no Azure
# -------------------------------------------------
conn_params = {
    'host': 'easypanel.manutencaoplus.com',
    'database': 'tnorte',
    'user': 'nortebi',
    'password': 'Marituba@4321!',
    'port': 5432
}

# -------------------------------------------------
# Nome da tabela de destino
# -------------------------------------------------
table_name = 'orcamento'

try:
    print("Tentando conectar ao banco...")
    conn = psycopg2.connect(**conn_params)
    print("Conectado com sucesso!")
    conn.autocommit = False
    cursor = conn.cursor()

    # -------------------------------------------------
    # Cria a tabela se não existir
    # -------------------------------------------------
    print(f"Verificando ou criando a tabela '{table_name}'...")
    create_table_query = sql.SQL(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            unidade TEXT,
            dia INT,
            data DATE,
            cte INT,
            valor DECIMAL(10,2),
            volume INT,
            peso DECIMAL(10,2)
        )
    """)
    cursor.execute(create_table_query)
    print(f"Tabela '{table_name}' verificada/criada com sucesso.")

    # -------------------------------------------------
    # Comando de inserção de dados
    # -------------------------------------------------
    print("Preparando o comando de inserção...")
    insert_query = sql.SQL("""
        INSERT INTO {table} (unidade, dia, data, cte, valor, volume, peso)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """).format(table=sql.Identifier(table_name))
    print("Comando de inserção preparado com sucesso.")

    # -------------------------------------------------
    # Inserção dos registros
    # Caso a coluna não exista no arquivo, inserir valor nulo
    # -------------------------------------------------
    print("Iniciando inserção de registros...")
    for index, row in df.iterrows():
        try:
            # Captura os valores de forma segura (None se colunas não existirem)
            unidade_val = safe_get_value(row, unidade_col, df)
            dia_val = safe_get_value(row, dia_col, df)
            data_val = safe_get_value(row, data_col, df)
            cte_val = safe_get_value(row, cte_col, df)
            valor_val = safe_get_value(row, valor_col, df)
            volume_val = safe_get_value(row, volume_col, df)
            peso_val = safe_get_value(row, peso_col, df)

            # Converte tipos numéricos caso não sejam None
            # DIA / CTE / VOLUME -> int
            dia_val = int(dia_val) if dia_val is not None else None
            cte_val = int(cte_val) if cte_val is not None else None
            volume_val = int(volume_val) if volume_val is not None else None

            # VALOR / PESO -> Decimal
            # Caso esteja None, permanece None
            if valor_val is not None:
                valor_val = Decimal(str(valor_val))
            if peso_val is not None:
                peso_val = Decimal(str(peso_val))

            # Executa a inserção
            cursor.execute(insert_query, (
                unidade_val,
                dia_val,
                data_val,
                cte_val,
                valor_val,
                volume_val,
                peso_val
            ))
            
        except Exception as e:
            print(f"Erro ao inserir linha {index + 1}: {row.to_dict()}, Erro: {e}")

    # Confirma as inserções
    conn.commit()
    print("Todos os dados foram inseridos com sucesso!")

except Exception as e:
    if 'conn' in locals():
        conn.rollback()
    print(f"Erro geral: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
        print("Cursor fechado.")
    if 'conn' in locals():
        conn.close()
        print("Conexão com o banco fechada.")


# -------------------------------------------------
# Referências:
# - [Pandas - Documentação Oficial](https://pandas.pydata.org/docs/)
# - [psycopg2 - Documentação Oficial](https://www.psycopg.org/docs/)
# -------------------------------------------------
