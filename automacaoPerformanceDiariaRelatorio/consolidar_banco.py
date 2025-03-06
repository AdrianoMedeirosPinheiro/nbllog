import os
import pandas as pd
from sqlalchemy import text, inspect, types
from datetime import datetime
import openpyxl
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def criar_tabela_se_nao_existir(engine, tabela_destino, df_template):
    """
    Cria a tabela no banco de dados caso não exista, usando o schema do DataFrame.
    """
    # Padroniza o nome da tabela para minúsculas
    tabela_destino = tabela_destino.lower()
    if inspect(engine).has_table(tabela_destino):
        print(f"A tabela {tabela_destino} já existe. Nenhuma criação executada.")
        return
    else:
        print(f"Criando a tabela {tabela_destino}...")

        # Mapeia os tipos de coluna de acordo com o DataFrame
        dtype_mapping = {}
        for col in df_template.columns:
            if col in ["NOPRAZO", "ATRASADO"]:
                dtype_mapping[col] = types.Numeric()
            elif col in ["PREVENTR", "DATA_DO_RELATORIO", "DATA_COMPETENCIA"]:
                dtype_mapping[col] = types.DateTime()
            elif col == "HORA_DO_RELATORIO":
                dtype_mapping[col] = types.Text()
            else:
                dtype_mapping[col] = types.Text()

        # Cria a tabela usando to_sql com if_exists='replace'
        df_template.to_sql(
            tabela_destino,
            con=engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping
        )
        print(f"Tabela {tabela_destino} criada com sucesso.")

def inserir_dados_usando_copy(df, tabela_destino, engine):
    """
    Insere os dados do DataFrame no banco de dados usando o método COPY (PostgreSQL).
    """
    tabela_destino = tabela_destino.lower()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    temp_filename = 'temp_data.csv'
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    # Substitui NaN por None
    df = df.where(df.notnull(), None)

    # Exporta para CSV sem header, pois COPY fará o mapeamento direto
    df.to_csv(
        temp_filename,
        sep=';',
        index=False,
        header=False,
        na_rep='',
        date_format='%Y-%m-%d'
    )

    try:
        with open(temp_filename, 'r', encoding='utf-8') as temp_file:
            cursor.execute("SET datestyle TO 'DMY';")
            copy_sql = f"""
                COPY {tabela_destino}
                FROM STDIN
                WITH (FORMAT csv, DELIMITER ';', NULL '')
            """
            cursor.copy_expert(copy_sql, temp_file)
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir dados com COPY: {e}")
    finally:
        cursor.close()
        conn.close()
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def processar_arquivo_excel(caminho_arquivo, tabela_destino, engine):
    """
    Lê o arquivo Excel e insere os dados no banco de dados,
    evitando a inserção duplicada. Se algum registro já existir (baseado
    na coluna DATA_DO_RELATORIO), nenhum dado será inserido.
    """
    # Padroniza o nome da tabela para minúsculas
    tabela_destino = tabela_destino.lower()

    if not os.path.isfile(caminho_arquivo):
        print(f"O arquivo {caminho_arquivo} não foi encontrado.")
        return

    try:
        # Colunas esperadas
        colunas = [
            "CTRC", "NF", "REMETENTE", "PAGADOR/ABC", "DESTINATARIO", "CIDADE DESTINO",
            "PREVENTR", "NOPRAZO", "ATRASADO", "--", "-5", "-4", "-3", "-2", "-1", "0",
            "+1", "+2", "+3", "+4", "+5", "+6", "+7", "+8", "+9",
            "DATA_DO_RELATORIO", "HORA_DO_RELATORIO", "MES_DO_RELATORIO",
            "DATA_COMPETENCIA", "UNIDADE", "RESPONSABILIDADE CLIENTE"
        ]

        # 1) Ler o arquivo Excel
        df = pd.read_excel(
            caminho_arquivo,
            engine='openpyxl',
            header=0
        )
        df = df.dropna(how='all')  # Remove linhas completamente vazias

        # Verifica se as colunas estão de acordo e renomeia se necessário
        if list(df.columns) != colunas:
            print("As colunas do Excel não correspondem às esperadas. Renomeando colunas...")
            df.columns = colunas
            print("Colunas renomeadas para:", df.columns.tolist())

        # Converte colunas numéricas
        for c in ["NOPRAZO", "ATRASADO"]:
            df[c] = pd.to_numeric(df[c], errors='coerce')

        # Converte colunas de data
        for c in ["PREVENTR", "DATA_DO_RELATORIO", "DATA_COMPETENCIA"]:
            df[c] = pd.to_datetime(df[c], errors='coerce', format='%d/%m/%y')

        # Verifica a existência da coluna DATA_DO_RELATORIO e datas válidas
        if "DATA_DO_RELATORIO" not in df.columns:
            print("Erro: Coluna 'DATA_DO_RELATORIO' não encontrada.")
            return
        if df["DATA_DO_RELATORIO"].isnull().all():
            print("Erro: Nenhuma data válida na coluna 'DATA_DO_RELATORIO'.")
            return

        # 2) Criar a tabela, se necessário
        criar_tabela_se_nao_existir(engine, tabela_destino, df)

        # 3) Obter todas as datas únicas do relatório
        datas_unicas = df["DATA_DO_RELATORIO"].dropna().unique()
        datas_param = [d.date() for d in datas_unicas]

        # 4) Verificar se já existe algum registro com essas datas
        registro_existente = False
        with engine.connect() as conn:
            for data in datas_param:
                query = text(f'''
                    SELECT 1
                    FROM {tabela_destino}
                    WHERE DATE("DATA_DO_RELATORIO") = :data
                    LIMIT 1
                ''')
                resultado = conn.execute(query, {"data": data}).fetchone()
                if resultado:
                    print(f"Registro com data {data} já existe no banco.")
                    registro_existente = True
                    break

        if registro_existente:
            print("Nenhum dado será inserido para evitar duplicidade.")
            return
        else:
            print("Nenhum registro duplicado encontrado. Inserindo os dados...")
            inserir_dados_usando_copy(df, tabela_destino, engine)

    except Exception as e:
        print(f"Erro ao processar o arquivo Excel: {e}")

def consolidar_resultado_processado(caminho_arquivo, tabela_destino, engine):
    """
    Função principal que processa (lê) o arquivo e insere os dados no banco de dados.
    """
    processar_arquivo_excel(caminho_arquivo, tabela_destino, engine)




# ===============================
# Configuração do Banco de Dados
# ===============================
# CONEXÃO COM BANCO AZURE ##

# # Defina os parâmetros do banco PostgreSQL
# diretorio = 'C:\\Users\\NBL\\Desktop\\Automacoes NBL\\automacao - Performance81\\base\\resultado_processado.xlsx'  # Caminho para o diretório dos arquivos .sswweb
# tabela_destino = 'relatoriosperformance81'  # Nome da tabela no PostgreSQL

# # Parâmetros de conexão
# usuario = "nbtssw"
# senha = "Marituba2030@"  # Substitua pela sua senha
# host = "nbtssw.postgres.database.azure.com"    # Exemplo: "nbl.postgres.database.azure.com"
# porta = 5432
# banco = "nbl"

# # Utilize URL.create para construir a string de conexão
# url_object = URL.create(
#     drivername="postgresql+psycopg2",
#     username=usuario,
#     password=senha,
#     host=host,
#     port=porta,
#     database=banco,
#     query={"sslmode": "require"}
# )

# try:
#     # Cria o engine usando o URL criado
#     engine = create_engine(url_object)

#     # Testa a conexão com o SQLAlchemy
#     with engine.connect() as conn:
#         print("Conexão com o PostgreSQL via SQLAlchemy estabelecida com sucesso!")
# except Exception as e:
#     print("Erro ao conectar via SQLAlchemy:", e)

# # Chama a função para processar e inserir os arquivos na tabela PostgreSQL
# consolidar_resultado_processado(diretorio, tabela_destino, engine)