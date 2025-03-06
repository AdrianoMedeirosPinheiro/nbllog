import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from sqlalchemy import Date, Time, Float, String, BigInteger
import psycopg2
import time
from datetime import datetime

def limpar_nome_colunas(colunas):
    """
    Função para limpar e padronizar os nomes das colunas.
    Remove espaços extras, caracteres não imprimíveis e converte para um padrão uniforme.
    """
    return (
        colunas
        .str.replace('\xa0', '', regex=False)  # Remove non-breaking spaces
        .str.strip()                            # Remove leading/trailing spaces
        .str.replace('/', '_', regex=False)     # Substitui '/' por '_'
        .str.replace(' ', '_', regex=False)     # Substitui espaços por '_'
        .str.lower()                            # Converte para minúsculas
    )

def criar_tabela_se_nao_existir(engine, tabela_destino, caminho_arquivo, chunk_size=50000):
    if inspect(engine).has_table(tabela_destino):
        print(f"A tabela {tabela_destino} já existe. Excluindo registros dos últimos 12 meses.")
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # Data de início: primeiro dia do mês atual no ano passado
                data_atual = datetime.now()
                ano_passado = data_atual.year - 1
                primeiro_dia_mes_ano_passado = f"{ano_passado}-{data_atual.month:02d}-01"

                # Executar o DELETE com base na lógica definida
                conn.execute(text(f"""
                    DELETE FROM {tabela_destino}
                    WHERE data_de_emissao >= :inicio
                      AND data_de_emissao <= CURRENT_DATE
                """), {"inicio": primeiro_dia_mes_ano_passado})

                trans.commit()
                print("Exclusão de registros dos últimos 12 meses concluída com sucesso.")

                result = conn.execute(text(f"SELECT COUNT(*) FROM {tabela_destino}")).fetchone()
                if result[0] != 0:
                    print(f"Aviso: {result[0]} registros ainda presentes após tentativa de exclusão.")
            except Exception as e:
                trans.rollback()
                print(f"Erro ao excluir registros: {e}")
                raise
    else:
        print(f"Criando a tabela {tabela_destino}...")
        try:
            # Ler apenas a primeira chunk para criar a tabela
            chunk = pd.read_csv(
                caminho_arquivo,
                delimiter=';',
                header=1,  # Usar a segunda linha como cabeçalho
                encoding='latin1',
                nrows=0,    # Ler apenas os nomes das colunas
                low_memory=False
            )
            # Limpar os nomes das colunas
            chunk.columns = limpar_nome_colunas(chunk.columns)
            print(f"Colunas após limpeza: {list(chunk.columns)}")
            
            # Remover colunas indesejadas
            colunas_indesejadas = ['1', 'unnamed:_74']
            for col in colunas_indesejadas:
                if col in chunk.columns:
                    chunk = chunk.drop(columns=[col])
                    print(f"Coluna '{col}' removida.")

            # Definir as colunas e seus tipos de dados
            date_columns = [
                'data_de_emissao',
                'data_de_autorizacao',
                'data_do_primeiro_manifesto',
                'data_do_ultimo_manifesto',
                'data_de_inclusao_da_ultima_ocorrencia',
                'data_da_ultima_ocorrencia',
                'data_do_cancelamento',
                'data_da_entrega_realizada',
                'data_do_ultimo_romaneio',
                'previsao_de_entrega',
                'entrega_programada'
            ]
            time_columns = ['hora_de_emissao']
            bigint_columns = [
                'cnpj_pagador',
                'cnpj_destinatario',
                'numero_da_nota_fiscal',
                'quantidade_de_volumes',
                'quantidade_de_dias_de_atraso',
                'codigo_da_ultima_ocorrencia'
            ]
            float_columns = [
                'peso_real_em_kg',
                'cubagem_em_m3',
                'valor_da_mercadoria',
                'valor_do_frete',
                'valor_do_frete_sem_icms',
                'base_de_calculo',
                'valor_do_icms',
                'valor_do_iss',
                'peso_calculado_em_kg',
                'latitude_da_ultima_ocorrencia',
                'longitude_da_ultima_ocorrencia'
            ]
            percentage_columns = ['aliquota']

            # Mapear os tipos de dados
            dtype_mapping = {}
            for col in chunk.columns:
                if col in date_columns:
                    dtype_mapping[col] = Date()
                elif col in time_columns:
                    dtype_mapping[col] = Time()
                elif col in bigint_columns:
                    dtype_mapping[col] = BigInteger()
                elif col in float_columns:
                    dtype_mapping[col] = Float()
                elif col in percentage_columns:
                    dtype_mapping[col] = Float()
                else:
                    dtype_mapping[col] = String()

            # Criar a tabela com os tipos de dados especificados
            chunk.head(0).to_sql(
                tabela_destino, 
                engine, 
                if_exists='replace', 
                index=False, 
                dtype=dtype_mapping
            )
            print(f"Tabela {tabela_destino} criada com sucesso.")
        except Exception as e:
            print(f"Erro ao criar a tabela {tabela_destino}: {e}")
            raise

def inserir_dados_usando_copy(df, tabela_destino, engine):
    conn = engine.raw_connection()
    cursor = conn.cursor()

    temp_filename = 'temp_data.csv'

    # Remove o arquivo temporário se ele já existir
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    # Exporta o DataFrame para um arquivo CSV temporário
    # As datas já estão em formato YYYY-MM-DD devido à conversão no pandas
    df.to_csv(temp_filename, sep=';', index=False, header=False, na_rep='')

    try:
        with open(temp_filename, 'r', encoding='latin1') as temp_file:
            cursor.copy_expert(
                f"COPY {tabela_destino} FROM STDIN WITH CSV DELIMITER ';' NULL ''",
                temp_file
            )
        conn.commit()
        print(f"Dados inseridos com sucesso na tabela {tabela_destino}.")
    except Exception as e:
        print(f"Erro ao inserir dados com COPY: {e}")
    finally:
        cursor.close()
        conn.close()
        if os.path.exists(temp_filename):
            os.remove(temp_filename)

def processar_e_inserir_arquivo(caminho_arquivo, tabela_destino, engine, chunk_size=50000):
    try:
        for chunk in pd.read_csv(
            caminho_arquivo,
            delimiter=';',
            header=1,  # Usar a segunda linha como cabeçalho
            encoding='latin1',
            chunksize=chunk_size,
            low_memory=False
        ):
            chunk = chunk.dropna(how='all')

            # Remover espaços nas colunas
            chunk.columns = limpar_nome_colunas(chunk.columns)
            print(f"Colunas após limpeza no arquivo {caminho_arquivo}: {list(chunk.columns)}")

            # Remover colunas indesejadas
            colunas_indesejadas = ['1', 'unnamed:_74']
            for col in colunas_indesejadas:
                if col in chunk.columns:
                    chunk = chunk.drop(columns=[col])
                    print(f"Coluna '{col}' removida.")

            # Verificar se a coluna essencial existe
            coluna_essencial = 'serie_numero_ctrc'
            if coluna_essencial not in chunk.columns:
                print(f"Erro: A coluna '{coluna_essencial}' não foi encontrada no arquivo {caminho_arquivo}.")
                print(f"Colunas disponíveis: {list(chunk.columns)}")
                raise KeyError([coluna_essencial])

            # Apenas aplicar str.replace e str.strip() nas colunas do tipo objeto (strings)
            object_cols = chunk.select_dtypes(include=['object']).columns
            # Remover espaços, non-breaking spaces e outros caracteres especiais nas colunas de string
            chunk[object_cols] = chunk[object_cols].apply(
                lambda x: x.str.replace('\xa0', '', regex=False).str.strip()
            )

            # Definir as colunas e seus tipos de dados
            date_columns = [
                'data_de_emissao',
                'data_de_autorizacao',
                'data_do_primeiro_manifesto',
                'data_do_ultimo_manifesto',
                'data_de_inclusao_da_ultima_ocorrencia',
                'data_da_ultima_ocorrencia',
                'data_do_cancelamento',
                'data_da_entrega_realizada',
                'data_do_ultimo_romaneio',
                'previsao_de_entrega',
                'entrega_programada'
            ]
            time_columns = ['hora_de_emissao']
            bigint_columns = [
                'cnpj_pagador',
                'cnpj_destinatario',
                'numero_da_nota_fiscal',
                'quantidade_de_volumes',
                'quantidade_de_dias_de_atraso',
                'codigo_da_ultima_ocorrencia'
            ]
            float_columns = [
                'peso_real_em_kg',
                'cubagem_em_m3',
                'valor_da_mercadoria',
                'valor_do_frete',
                'valor_do_frete_sem_icms',
                'base_de_calculo',
                'valor_do_icms',
                'valor_do_iss',
                'peso_calculado_em_kg',
                'latitude_da_ultima_ocorrencia',
                'longitude_da_ultima_ocorrencia'
            ]
            percentage_columns = ['aliquota']

            # Converter colunas de data
            for col in date_columns:
                if col in chunk.columns:
                    if chunk[col].dtype == 'object':
                        # Remover caracteres não numéricos, exceto '/'
                        chunk[col] = chunk[col].str.replace(r'[^0-9/]', '', regex=True)
                    # Converter para datetime com formato flexível e formatar para ISO
                    chunk[col] = pd.to_datetime(
                        chunk[col], 
                        dayfirst=True, 
                        errors='coerce'
                    ).dt.strftime('%Y-%m-%d')  # Formato ISO para PostgreSQL

            # Converter colunas de hora
            for col in time_columns:
                if col in chunk.columns:
                    if chunk[col].dtype == 'object':
                        # Remover caracteres não numéricos, exceto ':'
                        chunk[col] = chunk[col].str.replace(r'[^0-9:]', '', regex=True)
                    # Converter para time com formato '%H:%M' e adicionar segundos para compatibilidade
                    chunk[col] = pd.to_datetime(
                        chunk[col], 
                        format='%H:%M', 
                        errors='coerce'
                    ).dt.strftime('%H:%M:%S')  # Adicionar segundos para compatibilidade

            # Converter colunas bigint com valores nulos permitidos
            for col in bigint_columns:
                if col in chunk.columns:
                    if chunk[col].dtype == 'object':
                        # Remover separadores de milhar, vírgulas, espaços e non-breaking spaces
                        chunk[col] = chunk[col].str.replace('.', '', regex=False)\
                                                 .str.replace(',', '', regex=False)\
                                                 .str.replace(' ', '', regex=False)\
                                                 .str.replace('\xa0', '', regex=False)
                    # Converter para Int64 (permite nulos)
                    chunk[col] = pd.to_numeric(
                        chunk[col], 
                        errors='coerce'
                    ).astype('Int64')

            # Converter colunas float com valores nulos permitidos
            for col in float_columns:
                if col in chunk.columns:
                    if chunk[col].dtype == 'object':
                        # Remover separadores de milhar '.', substituir ',' por '.'
                        chunk[col] = chunk[col].str.replace('.', '', regex=False)\
                                                 .str.replace(',', '.', regex=False)\
                                                 .str.strip()
                    # Converter para float
                    chunk[col] = pd.to_numeric(
                        chunk[col], 
                        errors='coerce'
                    )

            # Converter colunas de porcentagem
            for col in percentage_columns:
                if col in chunk.columns:
                    if chunk[col].dtype == 'object':
                        # Remover '%' e substituir ',' por '.'
                        chunk[col] = chunk[col].str.replace('%', '', regex=False)\
                                                 .str.replace('.', '', regex=False)\
                                                 .str.replace(',', '.', regex=False)\
                                                 .str.strip()
                    # Converter para float e dividir por 100
                    chunk[col] = pd.to_numeric(
                        chunk[col], 
                        errors='coerce'
                    ) / 100.0

            # Remover linhas com valores nulos em colunas essenciais
            chunk = chunk.dropna(subset=[coluna_essencial])

            # Verificar e imprimir uma amostra das datas e horas após conversão
            existing_date_columns = [col for col in date_columns if col in chunk.columns]
            print(f"Dados após conversão de datas no arquivo {caminho_arquivo}:")
            print(chunk[existing_date_columns].head())

            existing_time_columns = [col for col in time_columns if col in chunk.columns]
            print(f"Dados após conversão de horas no arquivo {caminho_arquivo}:")
            print(chunk[existing_time_columns].head())

            # Verificar a coluna essencial
            print(f"Verificando a coluna '{coluna_essencial}' no arquivo {caminho_arquivo}:")
            print(chunk[coluna_essencial].head())

            # Verificar se as datas foram convertidas corretamente
            if 'data_de_emissao' in chunk.columns:
                num_valid_dates = chunk['data_de_emissao'].notna().sum()
                print(f"Número de datas válidas em 'data_de_emissao': {num_valid_dates}")

            if not chunk.empty:
                inserir_dados_usando_copy(chunk, tabela_destino, engine)
            else:
                print(f"Aviso: Todos os dados no chunk atual do arquivo {caminho_arquivo} estavam incompletos e foram ignorados.")
    except KeyError as e:
        print(f"Erro ao processar o arquivo {caminho_arquivo}: {e}")
    except Exception as e:
        print(f"Erro ao processar o arquivo {caminho_arquivo}: {e}")

def consolidar_arquivos_sswweb_para_postgres(diretorio, tabela_destino, engine, chunk_size=50000):
    arquivos_sswweb = [
        os.path.join(diretorio, f) 
        for f in os.listdir(diretorio) 
        if f.endswith('.sswweb')
    ]

    if arquivos_sswweb:
        criar_tabela_se_nao_existir(engine, tabela_destino, arquivos_sswweb[0], chunk_size)
        time.sleep(5)  # Aguarda para garantir que a tabela seja criada

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
# tabela_destino = 'relatorios455'  # Nome da tabela no PostgreSQL

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
