import pandas as pd
import re
import os
from io import StringIO

def ler_arquivo_ssw(caminho_arquivo):
    """
    Lê um arquivo .sswweb e retorna um DataFrame e o caminho do arquivo.
    Remove todas as linhas antes da primeira ocorrência de "--------------------------------------------+".
    """
    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        # Localizar a linha com a primeira ocorrência de "--------------------------------------------+"
        inicio_dados_index = None
        for i, linha in enumerate(linhas):
            if "--------------------------------------------+" in linha:
                inicio_dados_index = i + 1  # Ignorar a linha do marcador
                break

        if inicio_dados_index is None:
            print(f"ERRO: Marcador '--------------------------------------------+' não encontrado em {caminho_arquivo}.")
            return pd.DataFrame(), caminho_arquivo

        # Manter apenas as linhas a partir do marcador
        dados_linhas = linhas[inicio_dados_index:]
        
        # Definir colspecs com base no layout
        colspecs = [
            (0, 45), (45, 52), (52, 61), (61, 71), (71, 77),
            (77, 85), (85, 91), (91, 101), (101, 107), (107, 118),
            (118, 124), (124, 131), (131, 141), (141, 147), (147, 157), (157, 163)
        ]

        # Criar o DataFrame
        df = pd.read_fwf(StringIO("".join(dados_linhas)), colspecs=colspecs, header=None)
        df.columns = [
            "UNIDADE_DESTINO", "QUANTIDADE_CLIENTES", "QCTRCS_EXPEDIDOS", "ENTREGUE_QCTRC",
            "ENTREGUE_%", "NOPRAZO_QCTRC", "NOPRAZO_%", "ATRASCLI_QCTRC", "ATRASCLI_%",
            "ATRASTRANSP_QCTRC", "ATRASTRANSP_%", "PERF_%", "NOPRAZO_TOTAL_QCTRC",
            "NOPRAZO_TOTAL_%", "ATRASADO_QCTRC", "ATRASADO_%"
        ]

        # Remover linhas com valores não numéricos na coluna "QUANTIDADE_CLIENTES"
        df = df[df["QUANTIDADE_CLIENTES"].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

        # Remover linhas que contenham "UNIDADE DESTINO"
        df = df[~df["UNIDADE_DESTINO"].astype(str).str.contains("UNIDADE DESTINO", na=False)]
        df.reset_index(drop=True, inplace=True)

        return df, caminho_arquivo

    except Exception as e:
        print(f"Erro ao processar o arquivo {caminho_arquivo}: {e}")
        return pd.DataFrame(), caminho_arquivo


def preencher_unidade_destino(df):
    """
    Preenche os valores NaN na coluna 'UNIDADE_DESTINO' com o último valor não nulo.
    """
    try:
        df['UNIDADE_DESTINO'] = df['UNIDADE_DESTINO'].ffill()
        return df
    except Exception as e:
        print(f"Erro ao preencher 'UNIDADE_DESTINO': {e}")
        return df


def ler_linhas_ignoradas(caminho_arquivo):
    """
    Lê um arquivo SSW e retorna as primeiras 5 linhas ignoradas,
    além de extrair a data, hora e mês do relatório.
    """
    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        linhas_ignoradas = linhas[:5]
        df_ignoradas = pd.DataFrame(linhas_ignoradas, columns=['LINHA_IGNORADA'])

        # Extrair data e hora (segunda linha do arquivo)
        if len(linhas) > 1:
            segunda_linha = linhas[1]
            match_data_hora = re.search(r'(\d{2}/\d{2}/\d{2}) (\d{2}:\d{2})', segunda_linha)

            if match_data_hora:
                DATA_DO_RELATORIO = match_data_hora.group(1)
                HORA_DO_RELATORIO = match_data_hora.group(2)
            else:
                DATA_DO_RELATORIO = None
                HORA_DO_RELATORIO = None
        else:
            DATA_DO_RELATORIO = None
            HORA_DO_RELATORIO = None

        # Extrair mês (terceira linha do arquivo)
        if len(linhas) > 2:
            terceira_linha = linhas[2]
            match_mes = re.search(r'MES: (\d{2}/\d{2})', terceira_linha)
            MES_DO_RELATORIO = match_mes.group(1) if match_mes else None
        else:
            MES_DO_RELATORIO = None

        return df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO

    except Exception as e:
        print(f"Erro ao processar o arquivo para linhas ignoradas ({caminho_arquivo}): {e}")
        return pd.DataFrame(), None, None, None


def separar_valores(valor):
    """
    Extrai (quantidade, percentual) de uma string.
    Ex.: "123 45%" --> (123, 45.0)
    """
    try:
        if not isinstance(valor, str) or valor.lower().strip() == "nan":
            return 0, 0
        valor = valor.replace(',', '.').replace('%', '').strip()
        partes = valor.split()
        if len(partes) == 2:
            quantidade, percentual = partes
        elif len(partes) == 1:
            quantidade, percentual = "0", partes[0]
        else:
            return 0, 0
        quantidade = int(float(re.sub(r'[^\d\.\-]', '', quantidade)))
        percentual = float(re.sub(r'[^\d\.\-]', '', percentual))
        return quantidade, percentual
    except Exception as e:
        print(f"[ERROR] Erro ao processar valor: {valor}. Erro: {e}")
        return 0, 0


def processar_arquivos_ssw(diretorio):
    """
    Percorre todos os arquivos .sswweb no diretório, processa cada um
    e retorna um único DataFrame com a consolidação dos dados.
    """
    # Identificar todos os arquivos .sswweb no diretório
    arquivos = [f for f in os.listdir(diretorio) if f.lower().endswith('.sswweb')]
    if not arquivos:
        print("ERRO: Nenhum arquivo .sswweb encontrado no diretório.")
        return pd.DataFrame()

    lista_dataframes = []

    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio, arquivo)
        print(f"[PROCESSANDO]: {caminho_arquivo}")

        # Ler o arquivo e montar o DataFrame
        df, caminho_lido = ler_arquivo_ssw(caminho_arquivo)
        if df.empty:
            print(f"Não foi possível processar {caminho_arquivo}.\n")
            continue  # Pula para o próximo arquivo

        # Separar valores numéricos e percentuais
        colunas_para_separar = ['ENTREGUE_%', 'NOPRAZO_%', 'ATRASCLI_%', 'ATRASTRANSP_%']
        for coluna in colunas_para_separar:
            if coluna in df.columns:
                try:
                    df[f'Quantidade_{coluna.strip("_")}'], df[f'Percentual_{coluna.strip("_")}'] = zip(
                        *df[coluna].apply(separar_valores)
                    )
                except Exception as e:
                    print(f"[ERROR] Erro ao processar a coluna '{coluna}' em {arquivo}: {e}")

        # Preencher 'UNIDADE_DESTINO' se existir
        if 'UNIDADE_DESTINO' in df.columns:
            df = preencher_unidade_destino(df)
        else:
            print(f"ERRO: Coluna 'UNIDADE_DESTINO' não encontrada em {arquivo}.")

        # Remover pontos (.) das colunas numéricas (exceto as que terminam com '%')
        cols_to_clean = [col for col in df.columns[1:] if not col.endswith('%')]
        for col in cols_to_clean:
            df[col] = df[col].astype(str).str.replace('.', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Ler linhas ignoradas e extrair data/hora/mês
        df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO = ler_linhas_ignoradas(caminho_arquivo)
        df['DATA_DO_RELATORIO'] = DATA_DO_RELATORIO
        df['HORA_DO_RELATORIO'] = HORA_DO_RELATORIO
        df['MES_DO_RELATORIO'] = MES_DO_RELATORIO

        # Se quiser identificar o arquivo de origem em cada linha:
        df['ARQUIVO_ORIGEM'] = arquivo

        # Excluir colunas excedentes (16:24) se existirem
        # Obs.: atente para o índice das colunas
        if len(df.columns) > 16:
            # Aqui mantemos apenas as 16 primeiras colunas originais + colunas novas criadas
            # Ajuste conforme sua necessidade
            df.drop(df.columns[16:24], axis=1, inplace=True, errors='ignore')

        lista_dataframes.append(df)

    # Concatenar todos os dataframes gerados
    if lista_dataframes:
        df_final = pd.concat(lista_dataframes, ignore_index=True)
        print("\n[SUCESSO] DataFrame Final Unificado:")
        print(df_final.head())
        return df_final
    else:
        print("Nenhum DataFrame foi gerado. Verifique os arquivos.")
        return pd.DataFrame()

# ===============================
# Exemplo de chamada principal
# ===============================
if __name__ == "__main__":
    diretorio = r"C:\Users\NBL\Desktop\Automacoes NBL\automacao - Performance81\base"
    df_resultado = processar_arquivos_ssw(diretorio)
    if not df_resultado.empty:
        caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
        df_resultado.to_excel(caminho_saida, index=False)
        print(f"Arquivo Excel salvo em: {caminho_saida}")
