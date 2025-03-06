import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text, types
from sqlalchemy.engine import URL
import psycopg2
import time
from datetime import datetime

def criar_tabela_se_nao_existir(engine, tabela_destino, caminho_arquivo):
    if inspect(engine).has_table(tabela_destino):
        print(f"A tabela {tabela_destino} já existe. Excluindo registros dos últimos 9 meses.")
        with engine.connect() as conn:
            trans = conn.begin()  # Inicia uma transação
            try:
                # Data atual
                data_atual = datetime.now()
                
                # Calcular o mês e ano de dois meses atrás
                if data_atual.month > 9:
                    mes_inicio = data_atual.month - 9
                    ano_inicio = data_atual.year
                else:
                    mes_inicio = 12 - (9 - data_atual.month)
                    ano_inicio = data_atual.year - 1

                # Primeiro dia de dois meses atrás
                primeiro_dia_2_meses_atras = f"{ano_inicio}-{mes_inicio:02d}-01"

                # Executar a exclusão
                conn.execute(text(f"""
                    DELETE FROM {tabela_destino}
                    WHERE TO_DATE("MES COMPETENCIA", 'MM/YY') >= :inicio
                      AND TO_DATE("MES COMPETENCIA", 'MM/YY') <= CURRENT_DATE
                """), {"inicio": primeiro_dia_2_meses_atras})
                
                trans.commit()  # Confirma a transação
                print("Exclusão de registros dos últimos 9 meses concluída com sucesso.")
                
                # Verifica a contagem para confirmar que o DELETE foi bem-sucedido
                result = conn.execute(text(f"SELECT COUNT(*) FROM {tabela_destino}")).fetchone()
                if result[0] != 0:
                    print(f"Aviso: {result[0]} registros ainda presentes após tentativa de exclusão.")
            except Exception as e:
                trans.rollback()  # Reverte a transação em caso de erro
                print(f"Erro ao excluir registros: {e}")
                raise
    else:
        print(f"Criando a tabela {tabela_destino}...")
    
        # Lê o cabeçalho (primeira linha) do arquivo
        with open(caminho_arquivo, 'r', encoding='latin1') as f:
            header_line = f.readline()
            header = [col.strip() for col in header_line.strip().split(';') if col.strip() != '']
        
        # Verifica se há colunas com nomes em branco e as remove
        if '' in header:
            print("Aviso: Encontrado nome de coluna vazio. Removendo colunas com nomes vazios.")
            header = [col for col in header if col != '']
        
        # Cria um DataFrame vazio com as colunas corretas
        df_empty = pd.DataFrame(columns=header)
        print(f"Colunas do DataFrame vazio: {df_empty.columns.tolist()}")

        # Define os tipos de dados para as colunas
        dtype_mapping = {}
        # Colunas de inteiros ou decimais
        numeric_columns = ["EMPRESA", "NUMLANCTO", "PARCELA", "EVENTO", "CNPJ FORNECEDOR", "CNPJ FAVORECIDO", "NFISCAL", "CHAVE NFISCAL",
                           "VALOR TOTAL PRODUTOS", "VLR NOTA", "VLR PARCELA", "JUROS", "DESCONTOS", "VLR FINAL"]
        for col in numeric_columns:
            if col in df_empty.columns:
                dtype_mapping[col] = types.Numeric()

        # Colunas de datas
        date_columns = ["INCLUSAO", "VENCIMEN", "EMISSAO", "DATA PGTO", "CONTABIL"]
        for col in date_columns:
            if col in df_empty.columns:
                dtype_mapping[col] = types.Date()

        # Outras colunas serão tratadas como texto
        for col in df_empty.columns:
            if col not in dtype_mapping:
                dtype_mapping[col] = types.Text()

        # Cria a tabela no banco de dados usando o DataFrame vazio e os tipos definidos
        df_empty.to_sql(tabela_destino, engine, if_exists='replace', index=False, dtype=dtype_mapping)
        print(f"Tabela {tabela_destino} criada com sucesso com as colunas: {header}")

def inserir_dados_usando_copy(df, tabela_destino, engine):
    conn = engine.raw_connection()
    cursor = conn.cursor()

    temp_filename = 'temp_data.csv'

    # Remove o arquivo temporário se ele já existir
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    # Converte valores NaT para string vazia
    df = df.where(df.notnull(), None)
    df.to_csv(temp_filename, sep=';', index=False, header=False, na_rep='', date_format='%d/%m/%Y')

    try:
        with open(temp_filename, 'r', encoding='latin1') as temp_file:
            # **Define o datestyle para DMY**
            cursor.execute("SET datestyle TO 'DMY';")
            # Comando COPY corrigido
            copy_sql = f"COPY {tabela_destino} FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL '')"
            cursor.copy_expert(copy_sql, temp_file)
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir dados com COPY: {e}")
    finally:
        cursor.close()
        conn.close()
        # Remove o arquivo temporário ao final de cada execução
        if os.path.exists(temp_filename):
            os.remove(temp_filename)



def processar_e_inserir_arquivo(caminho_arquivo, tabela_destino, engine, chunk_size=50000):
    try:
        # Lê a primeira linha do arquivo para obter o cabeçalho
        with open(caminho_arquivo, 'r', encoding='latin1') as f:
            header_line = f.readline().strip()
            header = [col.strip() for col in header_line.split(';') if col.strip() != '']

        # Verificação para garantir que o cabeçalho não está vazio
        if not header:
            print(f"Aviso: O arquivo {caminho_arquivo} possui um cabeçalho vazio.")
            return

        # Opcional: imprimir os nomes das colunas para verificação
        print(f"Nomes das colunas: {header}")
        
        # Lê os dados ignorando as duas primeiras linhas (cabeçalho e segunda linha)
        for chunk in pd.read_csv(
            caminho_arquivo,
            delimiter=';',
            names=header,
            skiprows=2,  # Ignora as duas primeiras linhas
            encoding='latin1',
            chunksize=chunk_size,
            header=None,   # Não usar nenhuma linha como cabeçalho
            low_memory=False
        ):
            chunk = chunk.dropna(how='all')  # Remove linhas completamente vazias
            # Remover espaços em branco nas strings
            for col in chunk.select_dtypes(include=['object']).columns:
                chunk[col] = chunk[col].astype(str).str.strip()

            # Remove linhas onde 'DESCR EVENTO' é numérico
            if 'DESCR EVENTO' in chunk.columns:
                # Remove caracteres especiais e espaços
                chunk['DESCR EVENTO'] = chunk['DESCR EVENTO'].astype(str).str.replace(r'[^\w\s]', '', regex=True).str.strip()
                # Cria uma máscara booleana onde 'DESCR EVENTO' é numérico
                mask_numeric_descr_evento = chunk['DESCR EVENTO'].str.replace(' ', '').str.isdigit()
                # Filtra linhas onde 'DESCR EVENTO' não é numérico
                chunk = chunk[~mask_numeric_descr_evento]

            # Converter colunas específicas para numéricas (inteiros ou decimais)
            numeric_columns = ["EMPRESA", "NUMLANCTO", "PARCELA", "EVENTO", "CNPJ FORNECEDOR", "CNPJ FAVORECIDO", "NFISCAL", "CHAVE NFISCAL",
                               "VALOR TOTAL PRODUTOS", "VLR NOTA", "VLR PARCELA", "JUROS", "DESCONTOS", "VLR FINAL"]
            for col in numeric_columns:
                if col in chunk.columns:
                    chunk[col] = chunk[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    chunk[col] = chunk[col].str.replace(r'[^\d\.-]', '', regex=True)
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

            # Converter colunas específicas para data no formato dd/mm/aaaa
            date_columns = ["INCLUSAO", "VENCIMEN", "EMISSAO", "DATA PGTO", "CONTABIL"]
            for col in date_columns:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], format='%d/%m/%Y', errors='coerce')

            # Reordena as colunas de acordo com o cabeçalho
            chunk = chunk[header]

            # Remove linhas completamente vazias
            valid_rows = chunk.dropna(how='all')

            # Verifica se o chunk válido tem dados antes de inserir no banco
            if not valid_rows.empty:
                inserir_dados_usando_copy(valid_rows, tabela_destino, engine)
            else:
                print(f"Aviso: Todos os dados válidos no chunk atual do arquivo {caminho_arquivo} foram ignorados devido a inconsistências.")
    except Exception as e:
        print(f"Erro ao processar o arquivo {caminho_arquivo}: {e}")

def consolidar_arquivos_sswweb_para_postgres(diretorio, tabela_destino, engine, chunk_size=50000):
    arquivos_sswweb = [os.path.join(diretorio, f) for f in os.listdir(diretorio) if f.endswith('.sswweb')]

    if arquivos_sswweb:
        criar_tabela_se_nao_existir(engine, tabela_destino, arquivos_sswweb[0])
        time.sleep(20)

    with engine.connect() as conn:
        conn.execute(text(f"ALTER TABLE {tabela_destino} DISABLE TRIGGER ALL;"))

    for caminho_arquivo in arquivos_sswweb:
        print(f"Processando e inserindo dados do arquivo: {caminho_arquivo}")
        processar_e_inserir_arquivo(caminho_arquivo, tabela_destino, engine, chunk_size)

    with engine.connect() as conn:
        conn.execute(text(f"ALTER TABLE {tabela_destino} ENABLE TRIGGER ALL;"))

    print(f"Todos os arquivos .sswweb foram processados e inseridos na tabela {tabela_destino} no PostgreSQL.")


# # Defina os parâmetros do banco PostgreSQL
# diretorio = 'C:\\automacao_455\\base'  # Caminho para o diretório dos arquivos .sswweb
# tabela_destino = 'relatorios_consolidados'  # Nome da tabela no PostgreSQL

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
