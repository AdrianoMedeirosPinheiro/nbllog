import pandas as pd
import re
import os
from io import StringIO

def ler_arquivo_ssw(caminho_arquivo):
    """
    Lê e processa UM arquivo .sswweb (caminho completo).
    Remove tudo antes da linha '-------------------------------------------------------'
    e retorna um DataFrame bruto (sem concatenar com outros).
    """

    try:
        print(f"[DEBUG] Lendo arquivo individual: {caminho_arquivo}")
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        # Encontrar o índice da linha com '-------------------------------------------------------'
        inicio_dados = next(
            (i for i, linha in enumerate(linhas)
             if '-------------------------------------------------------' in linha),
            None
        )
        if inicio_dados is None:
            print("[ERROR] Marcador '-------------------------------------------------------' não encontrado.")
            return pd.DataFrame()  # Retorna vazio

        # Remove todas as linhas anteriores ao marcador
        linhas = linhas[inicio_dados + 1:]

        # Definir colspecs conforme layout do relatório
        colspecs = [
            (0, 55), (55, 62), (62, 71), (71, 82), (82, 88),
            (88, 96), (96, 102), (102, 112), (112, 120), (120, 129),
            (129, 136), (136, 147), (147, 154), (154, 160), (160, 170), (170, 177)
        ]

        # Criar DataFrame a partir do "miolo" do arquivo
        df = pd.read_fwf(StringIO("".join(linhas)), colspecs=colspecs, header=None)
        df.columns = [
            "UNIDADE_DESTINO", "QUANTIDADE_CLIENTES", "QCTRCS_EXPEDIDOS", "ENTREGUE_QCTRC",
            "ENTREGUE_%", "NOPRAZO_QCTRC", "NOPRAZO_%", "ATRASCLI_QCTRC", "ATRASCLI_%",
            "ATRASTRANSP_QCTRC", "ATRASTRANSP_%", "PERF_%", "NOPRAZO_TOTAL_QCTRC",
            "NOPRAZO_TOTAL_%", "ATRASADO_QCTRC", "ATRASADO_%"
        ]

        # Remove linhas não numéricas na coluna QUANTIDADE_CLIENTES
        df = df[
            df["QUANTIDADE_CLIENTES"].apply(
                lambda x: str(x).replace('.', '', 1).isdigit()
            )
        ]

        # Remove linhas que sejam cabeçalho repetido no meio
        df = df[~df["UNIDADE_DESTINO"]
                .astype(str)
                .str.contains("UNIDADE DESTINO", na=False)]
        df.reset_index(drop=True, inplace=True)

        # Retorna o df (que ainda não passou por todos os ajustes finais).
        return df

    except Exception as e:
        print(f"[ERROR] Erro ao processar o arquivo {caminho_arquivo}. Detalhes: {e}")
        return pd.DataFrame()


def preencher_unidade_destino(df):
    """
    Preenche valores NaN na coluna 'UNIDADE_DESTINO' com o último valor não nulo.
    """
    try:
        df['UNIDADE_DESTINO'] = df['UNIDADE_DESTINO'].ffill()
        return df
    except Exception as e:
        print(f"[ERROR] Erro ao preencher 'UNIDADE_DESTINO': {e}")
        return df


def ler_linhas_ignoradas(caminho_arquivo):
    """
    Lê um arquivo .sswweb e retorna as primeiras 5 linhas (linhas ignoradas),
    além de extrair data, hora e mês do relatório.
    """
    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        linhas_ignoradas = linhas[:5]
        df_ignoradas = pd.DataFrame(linhas_ignoradas, columns=['LINHA_IGNORADA'])

        # Extrair data/hora da segunda linha
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

        # Extrair mês da terceira linha
        if len(linhas) > 2:
            terceira_linha = linhas[2]
            match_mes = re.search(r'MES: (\d{2}/\d{2})', terceira_linha)
            MES_DO_RELATORIO = match_mes.group(1) if match_mes else None
        else:
            MES_DO_RELATORIO = None

        return df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO

    except Exception as e:
        print(f"[ERROR] Erro ao processar o arquivo para linhas ignoradas: {e}")
        return pd.DataFrame(), None, None, None


def separar_valores(valor):
    """
    Extrai (quantidade, percentual) de uma string. Ex.: '12 34,5' -> (12, 34.5)
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
        print(f"[ERROR] Erro ao processar valor: {valor}. Detalhes: {e}")
        return 0, 0


def processar_arquivos_ssw(diretorio):
    """
    Percorre TODOS os arquivos .sswweb do diretório,
    lê e trata cada um, e retorna um DataFrame concatenado.
    """
    # Lista todos os .sswweb do diretório
    arquivos_ssw = [f for f in os.listdir(diretorio) if f.lower().endswith('.sswweb')]
    if not arquivos_ssw:
        print("[ERROR] Nenhum arquivo .sswweb encontrado no diretório.")
        return pd.DataFrame()

    lista_dfs = []

    for arquivo in arquivos_ssw:
        caminho_arquivo = os.path.join(diretorio, arquivo)
        # Lê o DataFrame "bruto" do arquivo
        df = ler_arquivo_ssw(caminho_arquivo)
        if df.empty:
            # Se voltou vazio, pula
            continue

        # ============ TRATAMENTOS ADICIONAIS ============
        # 1) Extração de colunas de percentual (ENTREGUE_%, NOPRAZO_%, etc.)
        colunas_para_separar = ['ENTREGUE_%', 'NOPRAZO_%', 'ATRASCLI_%', 'ATRASTRANSP_%']
        for coluna in colunas_para_separar:
            if coluna in df.columns:
                try:
                    df[f'Quantidade_{coluna.strip("_") }'], df[f'Percentual_{coluna.strip("_") }'] = zip(
                        *df[coluna].apply(separar_valores)
                    )
                except Exception as e:
                    print(f"[ERROR] Erro ao processar a coluna '{coluna}': {e}")

        # 2) Preenche UNIDADE_DESTINO se existir a coluna
        if 'UNIDADE_DESTINO' in df.columns:
            df = preencher_unidade_destino(df)
        else:
            print("[ERROR] Coluna 'UNIDADE_DESTINO' não encontrada para preenchimento.")

        # 3) Remove pontos (.) das colunas que não terminam com '%'
        cols_to_clean = [col for col in df.columns[1:] if not col.endswith('%')]
        for col in cols_to_clean:
            df[col] = df[col].astype(str)
            df[col] = df[col].str.replace('.', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Converte para número

        # 4) Lê informações adicionais do cabeçalho (Data, Hora, Mês)
        df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO = ler_linhas_ignoradas(caminho_arquivo)
        df['DATA_DO_RELATORIO'] = DATA_DO_RELATORIO
        df['HORA_DO_RELATORIO'] = HORA_DO_RELATORIO
        df['MES_DO_RELATORIO'] = MES_DO_RELATORIO

        # 5) Exclui colunas desnecessárias (índices 16:24 - se existirem)
        if len(df.columns) > 16:
            df.drop(df.columns[16:24], axis=1, inplace=True, errors='ignore')

        lista_dfs.append(df)

    # Concatena todos os dataframes lidos
    if lista_dfs:
        df_concatenado = pd.concat(lista_dfs, ignore_index=True)
        print("[DEBUG] Concatenação final realizada com sucesso.")
        return df_concatenado
    else:
        print("[ERROR] Nenhum DataFrame processado com sucesso.")
        return pd.DataFrame()


# ------------------------------------------
# Exemplo de uso:
# if __name__ == "__main__":
#     diretorio = r'C:\\Users\\NBL\\Desktop\\Automacoes NBL\\automacao - Performance84\\base'
#     df_final = processar_arquivos_ssw(diretorio)
#     if not df_final.empty:
#         caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
#         df_final.to_excel(caminho_saida, index=False)
#         print(f"[DEBUG] Arquivo Excel salvo em: {caminho_saida}")
# ------------------------------------------
