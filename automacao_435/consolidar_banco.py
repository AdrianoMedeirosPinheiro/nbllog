import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text, types
from sqlalchemy.engine import URL
import time

def criar_tabela_se_nao_existir(engine, tabela_destino, caminho_arquivo):
    print(f"[DEBUG] Verificando se a tabela {tabela_destino} existe...")
    if inspect(engine).has_table(tabela_destino):
        print(f"[DEBUG] A tabela {tabela_destino} já existe. Excluindo registros.")
        with engine.connect() as conn:
            trans = conn.begin()  # Inicia uma transação
            time.sleep(10)
            try:
                print(f"[DEBUG] Executando DELETE na tabela {tabela_destino}...")
                conn.execute(text(f"""
                    DELETE FROM {tabela_destino}
                """))
                trans.commit()  # Confirma a transação
                print("[DEBUG] Exclusão de registros com sucesso.")
            except Exception as e:
                trans.rollback()  # Reverte a transação em caso de erro
                print(f"[DEBUG] Erro ao excluir registros: {e}")
                raise
    else:
        print(f"[DEBUG] Criando a tabela {tabela_destino}...")

        # Lê o cabeçalho (terceira linha) do arquivo
        with open(caminho_arquivo, 'r', encoding='latin1') as f:
            print(f"[DEBUG] Lendo cabeçalho do arquivo {caminho_arquivo}...")
            # Ignora as duas primeiras linhas
            f.readline()
            f.readline()
            header_line = f.readline()
            print(f"[DEBUG] Cabeçalho lido: {header_line}")
            header = [col.strip() for col in header_line.strip().split(';') if col.strip() != '']
            print(f"[DEBUG] Colunas extraídas: {header}")
            time.sleep(10)

        # Verifica se há colunas com nomes em branco e as remove
        if '' in header:
            print("[DEBUG] Aviso: Encontrado nome de coluna vazio. Removendo colunas com nomes vazios.")
            header = [col for col in header if col != '']
            print(f"[DEBUG] Colunas após remoção de nomes vazios: {header}")

        # Cria um DataFrame vazio com as colunas corretas
        df_empty = pd.DataFrame(columns=header)
        print(f"[DEBUG] Colunas do DataFrame vazio: {df_empty.columns.tolist()}")
        time.sleep(10)

        # Define os tipos de dados para as colunas
        dtype_mapping = {}
        # Colunas de inteiros ou decimais
        numeric_columns = ["FRETE", "ICMS", "CNPJ PAGADOR", "CNPJ REMETENTE", "CNPJ DESTINATARIO",
                           "NRO PEDIDO", "CHAVE CTE", "ULT. OCOR"]
        for col in numeric_columns:
            if col in df_empty.columns:
                dtype_mapping[col] = types.Numeric()
                print(f"[DEBUG] Definindo tipo NUMERIC para a coluna {col}.")

        # Colunas de datas
        date_columns = ["EMISSAO", "DATA ENTREGA", "PREVISAO DE ENTREGA", "ULT MVTO CLIENTE"]
        for col in date_columns:
            if col in df_empty.columns:
                dtype_mapping[col] = types.Date()
                print(f"[DEBUG] Definindo tipo DATE para a coluna {col}.")
        time.sleep(10)

        # Outras colunas serão tratadas como texto
        for col in df_empty.columns:
            if col not in dtype_mapping:
                dtype_mapping[col] = types.Text()
                print(f"[DEBUG] Definindo tipo TEXT para a coluna {col}.")

        # Cria a tabela no banco de dados usando o DataFrame vazio e os tipos definidos
        print(f"[DEBUG] Criando tabela {tabela_destino} no banco de dados...")
        df_empty.to_sql(tabela_destino, engine, if_exists='replace', index=False, dtype=dtype_mapping)
        print(f"[DEBUG] Tabela {tabela_destino} criada com sucesso com as colunas: {header}")

def inserir_dados_usando_copy(df, tabela_destino, engine):
    print(f"[DEBUG] Iniciando inserção de dados na tabela {tabela_destino}...")
    conn = engine.raw_connection()
    cursor = conn.cursor()
    time.sleep(10)

    temp_filename = 'temp_data.csv'
    print(f"[DEBUG] Arquivo temporário: {temp_filename}")

    # Remove o arquivo temporário se ele já existir
    if os.path.exists(temp_filename):
        print(f"[DEBUG] Removendo arquivo temporário existente: {temp_filename}")
        os.remove(temp_filename)
    time.sleep(10)

    # Converte valores NaT para string vazia
    df = df.where(df.notnull(), None)
    print(f"[DEBUG] Exportando DataFrame para arquivo temporário...")
    df.to_csv(temp_filename, sep=';', index=False, header=False, na_rep='', date_format='%Y-%m-%d')

    try:
        with open(temp_filename, 'r', encoding='latin1') as temp_file:
            print(f"[DEBUG] Executando comando COPY para inserir dados...")
            # Define o datestyle para ISO, YMD
            cursor.execute("SET datestyle TO 'ISO, YMD';")
            # Comando COPY
            copy_sql = f"COPY {tabela_destino} FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL '')"
            cursor.copy_expert(copy_sql, temp_file)
        conn.commit()
        print(f"[DEBUG] Dados inseridos com sucesso na tabela {tabela_destino}.")
    except Exception as e:
        print(f"[DEBUG] Erro ao inserir dados com COPY: {e}")
    finally:
        cursor.close()
        conn.close()
        time.sleep(10)
        # Remove o arquivo temporário ao final de cada execução
        if os.path.exists(temp_filename):
            print(f"[DEBUG] Removendo arquivo temporário: {temp_filename}")
            os.remove(temp_filename)

def processar_e_inserir_arquivo(caminho_arquivo, tabela_destino, engine, chunk_size=50000):
    print(f"[DEBUG] Processando arquivo: {caminho_arquivo}")
    try:
        # Lê a terceira linha do arquivo para obter o cabeçalho
        with open(caminho_arquivo, 'r', encoding='latin1') as f:
            print(f"[DEBUG] Lendo cabeçalho do arquivo {caminho_arquivo}...")
            # Ignora as duas primeiras linhas
            f.readline()
            f.readline()
            header_line = f.readline().strip()
            print(f"[DEBUG] Cabeçalho lido: {header_line}")
            header = [col.strip() for col in header_line.strip().split(';') if col.strip() != '']
            print(f"[DEBUG] Colunas extraídas: {header}")
            time.sleep(10)

        # Verificação para garantir que o cabeçalho não está vazio
        if not header:
            print(f"[DEBUG] Aviso: O arquivo {caminho_arquivo} possui um cabeçalho vazio.")
            return

        # Imprimir os nomes das colunas para verificação
        print(f"[DEBUG] Nomes das colunas: {header}")
        time.sleep(10)

        # Lê os dados a partir da quarta linha (dados começam na linha 4)
        for chunk in pd.read_csv(
            caminho_arquivo,
            sep=';',
            names=header,
            skiprows=3,  # Ignora as três primeiras linhas
            encoding='latin1',
            chunksize=chunk_size,
            header=None,   # Não usar nenhuma linha como cabeçalho
            engine='python'
        ):
            print(f"[DEBUG] Lendo chunk do arquivo {caminho_arquivo}...")
            chunk = chunk.dropna(how='all')  # Remove linhas completamente vazias
            # Remover espaços em branco nas strings
            for col in chunk.select_dtypes(include=['object']).columns:
                chunk[col] = chunk[col].astype(str).str.strip().replace({'': None})

            # Converter colunas específicas para numéricas (inteiros ou decimais)
            numeric_columns = ["FRETE", "ICMS", "CNPJ PAGADOR", "CNPJ REMETENTE", "CNPJ DESTINATARIO",
                                "NRO PEDIDO", "CHAVE CTE", "ULT. OCOR"]
            for col in numeric_columns:
                if col in chunk.columns:
                    # Substitui vírgula por ponto para conversão decimal
                    chunk[col] = chunk[col].astype(str).str.replace(',', '.', regex=False)
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                    print(f"[DEBUG] Coluna {col} convertida para numérico.")
            time.sleep(10)

            # Converter colunas específicas para data
            date_columns = ["EMISSAO", "DATA ENTREGA", "PREVISAO DE ENTREGA", "ULT MVTO CLIENTE"]
            for col in date_columns:
                if col in chunk.columns:
                    # Remover espaços em branco e valores vazios
                    chunk[col] = chunk[col].astype(str).str.strip().replace({'': None})
                    # Converter para datetime sem especificar o formato
                    chunk[col] = pd.to_datetime(chunk[col], errors='coerce', dayfirst=True, infer_datetime_format=True)
                    # Verificar a conversão
                    print(f"[DEBUG] Coluna {col} após conversão:")
                    print(chunk[col].head())
                    null_count = chunk[col].isnull().sum()
                    print(f"[DEBUG] Coluna {col} possui {null_count} valores nulos após conversão.")

            # Reordena as colunas de acordo com o cabeçalho
            chunk = chunk[header]

            # Remove linhas completamente vazias
            valid_rows = chunk.dropna(how='all')

            # Verifica se o chunk válido tem dados antes de inserir no banco
            if not valid_rows.empty:
                print(f"[DEBUG] Inserindo chunk válido na tabela {tabela_destino}...")
                inserir_dados_usando_copy(valid_rows, tabela_destino, engine)
            else:
                print(f"[DEBUG] Aviso: Todos os dados válidos no chunk atual do arquivo {caminho_arquivo} foram ignorados devido a inconsistências.")
    except Exception as e:
        print(f"[DEBUG] Erro ao processar o arquivo {caminho_arquivo}: {e}")

def consolidar_arquivos_sswweb_para_postgres(diretorio, tabela_destino, engine, chunk_size=50000):
    print(f"[DEBUG] Consolidando arquivos .sswweb no diretório {diretorio}...")
    arquivos = [os.path.join(diretorio, f) for f in os.listdir(diretorio) if f.endswith('.sswweb')]
    print(f"[DEBUG] Arquivos encontrados: {arquivos}")

    if arquivos:
        print(f"[DEBUG] Criando tabela {tabela_destino} se não existir...")
        criar_tabela_se_nao_existir(engine, tabela_destino, arquivos[0])
        time.sleep(20)

    with engine.connect() as conn:
        print(f"[DEBUG] Desabilitando triggers da tabela {tabela_destino}...")
        time.sleep(10)
        conn.execute(text(f"ALTER TABLE {tabela_destino} DISABLE TRIGGER ALL;"))

    for caminho_arquivo in arquivos:
        print(f"[DEBUG] Processando e inserindo dados do arquivo: {caminho_arquivo}")
        time.sleep(10)
        processar_e_inserir_arquivo(caminho_arquivo, tabela_destino, engine, chunk_size)

    with engine.connect() as conn:
        print(f"[DEBUG] Habilitando triggers da tabela {tabela_destino}...")
        time.sleep(10)
        conn.execute(text(f"ALTER TABLE {tabela_destino} ENABLE TRIGGER ALL;"))

    print(f"[DEBUG] Todos os arquivos foram processados e inseridos na tabela {tabela_destino} no PostgreSQL.")


# # Defina os parâmetros do banco PostgreSQL
# diretorio = 'C:\\automacao_435\\base'  # Caminho para o diretório dos arquivos .sswweb
# tabela_destino = 'relatorios435'  # Nome da tabela no PostgreSQL

# # Parâmetros de conexão
# usuario = "nblbi"
# senha = "Marituba2030@"  # Substitua pela sua senha
# host = "nbl.postgres.database.azure.com"    # Exemplo: "nbl.postgres.database.azure.com"
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
# consolidar_arquivos_sswweb_para_postgres(diretorio, tabela_destino, engine)
