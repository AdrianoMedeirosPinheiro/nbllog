import pandas as pd
import os
from sqlalchemy import create_engine, inspect, text, types
from sqlalchemy.engine import URL
from datetime import datetime

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

        # Cria a tabela com o mesmo schema do DataFrame
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
    conn = engine.raw_connection()
    cursor = conn.cursor()

    temp_filename = 'temp_data.csv'
    if os.path.exists(temp_filename):
        os.remove(temp_filename)

    # Substituir valores nulos por None
    df = df.where(df.notnull(), None)

    # Gera CSV temporário
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
            copy_sql = (
                f"COPY {tabela_destino} FROM STDIN "
                "WITH (FORMAT csv, DELIMITER ';', NULL '')"
            )
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
    Cria as colunas UNIDADE, CIDADE, PERIODO, etc.
    Evita inserir duplicados comparando TODAS as datas existentes no DataFrame com as do banco.
    """
    try:
        # Ler o arquivo Excel
        df = pd.read_excel(caminho_arquivo, engine='openpyxl')
        df = df.dropna(how='all')  # Remove linhas completamente vazias

        # Limpa espaços das colunas de texto
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()

        # Definir colunas numéricas
        numeric_columns = [
            "QUANTIDADE_CLIENTES", "QCTRCS_EXPEDIDOS", "ENTREGUE_QCTRC", "ENTREGUE_%",
            "NOPRAZO_QCTRC", "NOPRAZO_%", "ATRASCLI_QCTRC", "ATRASCLI_%",
            "ATRASTRANSP_QCTRC", "ATRASTRANSP_%", "PERF_%", "NOPRAZO_TOTAL_QCTRC",
            "NOPRAZO_TOTAL_%", "ATRASADO_QCTRC", "ATRASADO_%"
        ]

        # Colunas percentuais X inteiras
        percentage_columns = [col for col in numeric_columns if '%' in col]
        integer_columns = [col for col in numeric_columns if '%' not in col]

        def limpar_percentual(valor):
            if pd.isnull(valor):
                return None
            try:
                # Remove pontos e troca vírgula por ponto
                valor = str(valor).replace('.', '').replace(',', '.')
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
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # --------------------------------------------------
        # Mapeamento de siglas para cidade
        sigla_para_cidade = {
            # BEL
            "MTZ": "BEL",
            "CEN": "BEL",
            "EDN": "BEL",
            "IGM": "BEL",
            "JSP": "BEL",
            "MOJ": "BEL",
            "NPL": "BEL",
            "NIL": "BEL",
            "NST": "BEL",
            "ABT": "BEL",
            "BAR": "BEL",
            "CAS": "BEL",
            "DEL": "BEL",
            "ANA": "BEL",
            "CJO": "BEL",
            "BEL": "BEL",
            "TNL": "BEL",
            "TEC": "BEL",
            "SAT": "BEL",

            # MAB
            "MAB": "MAB",
            "MEL": "MAB",
            "BTR": "MAB",
            "JGL": "MAB",
            "MRA": "MAB",
            "TUC": "MAB",
            "NPP": "MAB",
            "RSD": "MAB",

            # FLU
            "AFC": "FLU",
            "ARC": "FLU",
            "BLT": "FLU",
            "BOJ": "FLU",
            "GLM": "FLU",
            "JSN": "FLU",
            "NHA": "FLU",
            "PLF": "FLU",
            "SDG": "FLU",
            "SMT": "FLU",
            "VCS": "FLU",
            "ZEN": "FLU",
            "GLF": "FLU",
            "GUR": "FLU",
            "MAT": "FLU",
            "MRC": "FLU",
            "RSM": "FLU",
            "RMA": "FLU",
            "TEG": "FLU",
            "OST": "FLU",

            # AM
            "SPN": "AM",
            "BVA": "AM",
            "BVB": "AM",
            "HMB": "AM",

            # AP
            "PNT": "AP",
            "NIS": "AP",

            # GYN
            "TGO": "GYN",
            "TNC": "GYN",
            "TG4": "GYN",
            "TG3": "GYN",
            "TG2": "GYN",
            "TG1": "GYN",

            # TO
            "MEM": "TO",
            "TOP": "TO"
        }


        # Criar as novas colunas vazias
        df["UNIDADE"] = None
        df["CIDADE"] = None

        # Preencher as novas colunas com base na primeira coluna
        primeira_coluna = df.columns[0]
        for i in range(len(df)):
            valor_primeira_coluna = str(df.iloc[i][primeira_coluna]).strip().upper()
            if not valor_primeira_coluna:
                continue

            possivel_sigla = valor_primeira_coluna.split()[0]
            if possivel_sigla in sigla_para_cidade:
                df.at[i, "UNIDADE"] = possivel_sigla
                df.at[i, "CIDADE"] = sigla_para_cidade[possivel_sigla]
        # --------------------------------------------------

        # Verificar a coluna DATA_DO_RELATORIO
        if "DATA_DO_RELATORIO" not in df.columns:
            print("Erro: Coluna 'DATA_DO_RELATORIO' não encontrada no arquivo Excel.")
            return

        # Converter para datetime
        df["DATA_DO_RELATORIO"] = pd.to_datetime(
            df["DATA_DO_RELATORIO"],
            errors='coerce',
            dayfirst=True
        )

        if df["DATA_DO_RELATORIO"].isnull().all():
            print("Erro: Não foi possível interpretar as datas na coluna 'DATA_DO_RELATORIO'.")
            return

        # Criar a coluna PERIODO
        df["PERIODO"] = df["DATA_DO_RELATORIO"].dt.day.map({
            20: "Primeira Parcial",
            1: "Segunda Parcial",
            10: "Final"
        })

        # ----------------- AJUSTE AQUI -------------------
        # 1. Se a tabela não existir, cria agora. 
        #    Assim, a consulta seguinte não dá erro de "relation does not exist".
        if not inspect(engine).has_table(tabela_destino):
            criar_tabela_se_nao_existir(engine, tabela_destino, df)

        # 2. Agora podemos consultar duplicados com segurança
        datas_unicas = df["DATA_DO_RELATORIO"].dropna().unique()
        datas_existentes = []
        with engine.connect() as conn:
            for d in datas_unicas:
                query = text(f"""
                    SELECT 1
                    FROM "{tabela_destino}"
                    WHERE DATE("DATA_DO_RELATORIO") = :data_relatorio
                    LIMIT 1
                """)
                resultado = conn.execute(query, {"data_relatorio": d.date()}).fetchone()
                if resultado:
                    datas_existentes.append(d)

        # 3. Filtrar apenas as linhas que não estão no banco
        df_filtrado = df[~df["DATA_DO_RELATORIO"].isin(datas_existentes)]
        if df_filtrado.empty:
            print("Todas as datas deste arquivo já estão no banco. Nenhum dado será inserido.")
            return

        # 4. Inserir os dados novos
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