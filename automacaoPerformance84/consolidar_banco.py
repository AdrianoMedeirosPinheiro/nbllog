import os
import pandas as pd
from sqlalchemy import text, inspect
from sqlalchemy import types
from datetime import datetime
import openpyxl
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def criar_tabela_se_nao_existir(engine, tabela_destino, df_template):
    """
    Cria a tabela no banco de dados caso não exista.
    """
    if inspect(engine).has_table(tabela_destino):
        print(f"A tabela {tabela_destino} já existe. Nenhuma criação executada.")
        return
    else:
        print(f"Criando a tabela {tabela_destino}...")

        # Mapear tipos de colunas
        dtype_mapping = {}
        for col in df_template.columns:
            if df_template[col].dtype in ['float64', 'int64']:
                dtype_mapping[col] = types.Numeric()
            elif df_template[col].dtype == 'datetime64[ns]':
                dtype_mapping[col] = types.DateTime()
            else:
                dtype_mapping[col] = types.Text()

        # Cria a tabela usando o schema do DataFrame
        df_template.to_sql(
            tabela_destino, 
            engine, 
            if_exists='replace', 
            index=False, 
            dtype=dtype_mapping
        )
        print(f"Tabela {tabela_destino} criada com sucesso.")


def inserir_dados_usando_copy(df, tabela_destino, engine):
    """
    Insere os dados do DataFrame no banco de dados usando o método COPY.
    """
    import os
    conn = engine.raw_connection()
    cursor = conn.cursor()

    temp_filename = 'temp_data.csv'
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    # Substituir valores NaN por None
    df = df.where(df.notnull(), None)

    # Exportar para CSV (sem header, pois COPY fará mapeamento direto)
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
            # Definir estilo de data no lado do banco
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
    Lê o arquivo Excel e insere os dados no banco de dados.
    Cria colunas UNIDADE e CIDADE a partir das siglas.
    Faz limpeza de colunas numéricas e formata datas.
    Evita inserir duplicados filtrando TODAS as datas que já existem no banco.
    """
    try:
        # 1) Ler o arquivo Excel
        df = pd.read_excel(caminho_arquivo, engine='openpyxl')
        df = df.dropna(how='all')  # Remove linhas completamente vazias

        # 2) Limpa espaços nas colunas de texto
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()

        # 3) Criar as colunas UNIDADE e CIDADE
        siglas_dict = {
            'BEL': ['MTZ', 'CEN', 'EDN', 'IGM', 'JSP', 'MOJ', 'NPL', 'NIL', 'NST', 'ABT', 'BAR', 'CAS', 'DEL', 'ANA', 'CJO', 'BEL', 'TNL', 'TEC', 'SAT'],
            'MAB': ['MAB', 'MEL', 'BTR', 'JGL', 'MRA', 'TUC', 'NPP', 'RSD'],
            'FLU': ['AFC', 'ARC', 'BLT', 'BOJ', 'GLM', 'JSN', 'NHA', 'PLF', 'SDG', 'SMT', 'VCS', 'ZEN', 'GLF', 'GUR', 'MAT', 'MRC', 'RSM', 'RMA', 'TEG', 'OST'],
            'AM':  ['SPN', 'BVA', 'BVB', 'HMB'],
            'AP':  ['PNT', 'NIS'],
            'GYN': ['TGO', 'TNC', 'TG4', 'TG3', 'TG2', 'TG1'],
            'TO':  ['MEM', 'TOP']
        }

        def encontrar_unidade_cidade(valor):
            """
            Verifica se 'valor' (string) começa com alguma das siglas definidas.
            Retorna (sigla_encontrada, cidade_correspondente) se achar,
            caso contrário (None, None).
            """
            valor_str = str(valor).upper()
            for cidade, lista_siglas in siglas_dict.items():
                for sigla in lista_siglas:
                    sigla_upper = sigla.upper()
                    if valor_str.startswith(sigla_upper):
                        return sigla, cidade
            return None, None

        df['UNIDADE'], df['CIDADE'] = zip(*df[df.columns[0]].apply(encontrar_unidade_cidade))

        # 4) Limpar colunas numéricas
        numeric_columns = [
            "QUANTIDADE_CLIENTES", "QCTRCS_EXPEDIDOS", "ENTREGUE_QCTRC", "ENTREGUE_%",
            "NOPRAZO_QCTRC", "NOPRAZO_%", "ATRASCLI_QCTRC", "ATRASCLI_%",
            "ATRASTRANSP_QCTRC", "ATRASTRANSP_%", "PERF_%", "NOPRAZO_TOTAL_QCTRC",
            "NOPRAZO_TOTAL_%", "ATRASADO_QCTRC", "ATRASADO_%"
        ]

        percentage_columns = [col for col in numeric_columns if '%' in col]
        integer_columns = [col for col in numeric_columns if '%' not in col]

        def limpar_percentual(valor):
            if pd.isnull(valor):
                return None
            try:
                valor = str(valor).replace('.', '').replace(',', '.')
                return pd.to_numeric(valor, errors='coerce')
            except ValueError:
                return None

        def limpar_inteiro(valor):
            if pd.isnull(valor):
                return None
            try:
                return pd.to_numeric(valor, errors='coerce')
            except ValueError:
                return None

        # Limpar colunas percentuais
        for col in percentage_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).apply(limpar_percentual)

        # Limpar colunas inteiras
        for col in integer_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).apply(limpar_inteiro)

        # 5) Verificar coluna DATA_DO_RELATORIO
        if "DATA_DO_RELATORIO" not in df.columns:
            print("Erro: Coluna 'DATA_DO_RELATORIO' não encontrada no arquivo Excel.")
            return

        # Converter para datetime (%d/%m/%y) ou outro formato conforme necessidade
        df["DATA_DO_RELATORIO"] = pd.to_datetime(
            df["DATA_DO_RELATORIO"],
            errors='coerce',
            format='%d/%m/%y'  # Ajuste conforme o formato real do Excel
        )

        if df["DATA_DO_RELATORIO"].isnull().all():
            print("Erro: Não foi possível interpretar as datas na coluna 'DATA_DO_RELATORIO'.")
            return

        # Criar coluna PERIODO
        df["PERIODO"] = df["DATA_DO_RELATORIO"].dt.day.map({
            20: "Primeira Parcial",
            1: "Segunda Parcial",
            10: "Final"
        })

        # ---------------------------
        # EVITAR DUPLICAÇÃO DE DADOS
        # ---------------------------

        # 1) Se a tabela ainda não existe, vamos criá-la agora
        #    para evitar erro de "relation does not exist" durante consulta.
        if not inspect(engine).has_table(tabela_destino):
            criar_tabela_se_nao_existir(engine, tabela_destino, df)

        # 2) Consultar as datas únicas do DF
        datas_unicas = df["DATA_DO_RELATORIO"].dropna().unique()

        # 3) Descobrir quais dessas datas já existem no banco
        datas_existentes = []
        with engine.connect() as conn:
            for d in datas_unicas:
                query = text(f'''
                    SELECT 1
                    FROM "{tabela_destino}"
                    WHERE DATE("DATA_DO_RELATORIO") = :data
                    LIMIT 1
                ''')
                # date() retira a hora, caso exista
                resultado = conn.execute(query, {"data": d.date()}).fetchone()
                if resultado:
                    datas_existentes.append(d)

        # 4) Filtrar apenas as linhas cujas datas NÃO estão no banco
        df_filtrado = df[~df["DATA_DO_RELATORIO"].isin(datas_existentes)]

        if df_filtrado.empty:
            print("Todas as datas deste arquivo já estão no banco. Nenhum dado será inserido.")
            return

        # 5) Inserir apenas as linhas "novas"
        inserir_dados_usando_copy(df_filtrado, tabela_destino, engine)

    except Exception as e:
        print(f"Erro ao processar o arquivo Excel: {e}")


def consolidar_resultado_processado(caminho_arquivo, tabela_destino, engine):
    """
    Função principal que processa o arquivo Excel e insere os dados no banco de dados.
    """
    if not os.path.isfile(caminho_arquivo):
        print(f"O arquivo {caminho_arquivo} não foi encontrado.")
        return

    processar_arquivo_excel(caminho_arquivo, tabela_destino, engine)



## CONEXÃO COM BANCO AZURE ##

# # Defina os parâmetros do banco PostgreSQL
# diretorio = 'C:\\Users\\NBL\\Desktop\\Automacoes NBL\\automacao - Performance84\\base\\resultado_processado.xlsx'  # Caminho para o diretório dos arquivos .sswweb
# tabela_destino = 'relatoriosperformance84'  # Nome da tabela no PostgreSQL

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