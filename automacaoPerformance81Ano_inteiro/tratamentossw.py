import pandas as pd
import re
import os
from io import StringIO

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

        if len(linhas) > 2:
            terceira_linha = linhas[2]
            match_mes = re.search(r'MES: (\d{2}/\d{2})', terceira_linha)
            MES_DO_RELATORIO = match_mes.group(1) if match_mes else None
        else:
            MES_DO_RELATORIO = None

        print(f"Data do Relatório: {DATA_DO_RELATORIO}")
        print(f"Hora do Relatório: {HORA_DO_RELATORIO}")
        print(f"Mês do Relatório: {MES_DO_RELATORIO}")

        return df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO

    except Exception as e:
        print(f"Erro ao processar o arquivo para linhas ignoradas: {e}")
        return pd.DataFrame(), None, None, None


def preencher_unidade_destino(df):
    """
    Preenche os valores NaN na coluna 'UNIDADE_DESTINO' com o último valor não nulo.
    """
    try:
        df['UNIDADE_DESTINO'] = df['UNIDADE_DESTINO'].ffill()
        print("Coluna 'UNIDADE_DESTINO' preenchida com valores anteriores.")
        return df
    except Exception as e:
        print(f"Erro ao preencher 'UNIDADE_DESTINO': {e}")
        return df


def separar_valores(valor):
    """
    Extrai (quantidade, percentual) de uma string.
    Exemplo de entrada: "120 85,3%"
    Retorna: (120, 85.3)
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


def ler_arquivo_ssw_unico(caminho_arquivo):
    """
    Lê e processa um único arquivo .sswweb, retornando um DataFrame.
    Remove todas as linhas antes da primeira ocorrência de "--------------------------------------------+".
    Também extrai informações de data/hora/mês.
    """
    try:
        print(f"\n[DEBUG] Processando arquivo: {caminho_arquivo}")
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        # Localiza o marcador de início
        inicio_dados_index = None
        for i, linha in enumerate(linhas):
            if "--------------------------------------------+" in linha:
                inicio_dados_index = i + 1
                break

        if inicio_dados_index is None:
            print("ERRO: Marcador '--------------------------------------------+' não encontrado.")
            return pd.DataFrame()

        dados_linhas = linhas[inicio_dados_index:]

        # Definir colspecs com base no layout
        colspecs = [
            (0, 45), (45, 52), (52, 61), (61, 71), (71, 77),
            (77, 85), (85, 91), (91, 101), (101, 107), (107, 118),
            (118, 124), (124, 131), (131, 141), (141, 147), (147, 157), (157, 163)
        ]

        df = pd.read_fwf(StringIO("".join(dados_linhas)), colspecs=colspecs, header=None)
        df.columns = [
            "UNIDADE_DESTINO", "QUANTIDADE_CLIENTES", "QCTRCS_EXPEDIDOS", "ENTREGUE_QCTRC",
            "ENTREGUE_%", "NOPRAZO_QCTRC", "NOPRAZO_%", "ATRASCLI_QCTRC", "ATRASCLI_%",
            "ATRASTRANSP_QCTRC", "ATRASTRANSP_%", "PERF_%", "NOPRAZO_TOTAL_QCTRC",
            "NOPRAZO_TOTAL_%", "ATRASADO_QCTRC", "ATRASADO_%"
        ]

        # Remover linhas com valores não numéricos em QUANTIDADE_CLIENTES
        df = df[df["QUANTIDADE_CLIENTES"].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

        # Remover linhas que contêm 'UNIDADE DESTINO' no texto para limpar
        df = df[~df["UNIDADE_DESTINO"].astype(str).str.contains("UNIDADE DESTINO", na=False)]
        df.reset_index(drop=True, inplace=True)

        # Converter colunas com "%" em quantidades e percentuais
        colunas_para_separar = ['ENTREGUE_%', 'NOPRAZO_%', 'ATRASCLI_%', 'ATRASTRANSP_%']
        for coluna in colunas_para_separar:
            if coluna in df.columns:
                df[f'Quantidade_{coluna.strip("_")}'], df[f'Percentual_{coluna.strip("_")}'] = zip(
                    *df[coluna].apply(separar_valores)
                )

        # Preenche UNIDADE_DESTINO
        if 'UNIDADE_DESTINO' in df.columns:
            df = preencher_unidade_destino(df)
        else:
            print("ERRO: Coluna 'UNIDADE_DESTINO' não encontrada para preenchimento.")

        # Remover pontos de colunas (transformar "1.234" em "1234")
        cols_to_clean = [col for col in df.columns[1:] if not col.endswith('%')]
        for col in cols_to_clean:
            df[col] = df[col].astype(str)
            df[col] = df[col].str.replace('.', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')  # consertar se der erro

        # Ler linhas ignoradas para obter data/hora/mês do relatório
        df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO = ler_linhas_ignoradas(caminho_arquivo)
        df['DATA_DO_RELATORIO'] = DATA_DO_RELATORIO
        df['HORA_DO_RELATORIO'] = HORA_DO_RELATORIO
        df['MES_DO_RELATORIO'] = MES_DO_RELATORIO

        # Remove colunas extras (se quiser manter, é só comentar)
        df.drop(df.columns[16:24], axis=1, inplace=True)

        return df

    except Exception as e:
        print(f"Erro ao processar o arquivo '{caminho_arquivo}': {e}")
        return pd.DataFrame()


def processar_arquivos_ssw(diretorio):
    """
    Percorre todos os arquivos .sswweb no diretório,
    processa cada um deles e concatena em um único DataFrame final.
    """
    arquivos = [f for f in os.listdir(diretorio) if f.lower().endswith('.sswweb')]
    if not arquivos:
        print("ERRO: Nenhum arquivo .sswweb encontrado no diretório.")
        return pd.DataFrame()

    lista_dfs = []
    for arq in arquivos:
        caminho_arquivo = os.path.join(diretorio, arq)
        df_parcial = ler_arquivo_ssw_unico(caminho_arquivo)
        if not df_parcial.empty:
            # Adicionar nome do arquivo como referência se quiser
            # df_parcial['ARQUIVO_ORIGEM'] = arq
            lista_dfs.append(df_parcial)

    if not lista_dfs:
        print("Nenhum DataFrame válido foi gerado.")
        return pd.DataFrame()

    # Concatena tudo em um único DataFrame
    df_final = pd.concat(lista_dfs, ignore_index=True)
    return df_final


# ===============================
# Exemplo de uso principal
# ===============================
# if __name__ == "__main__":
#     diretorio = r'C:\\Users\\NBL\\Desktop\\Automacoes NBL\\automacao - Performance81\\base'
#     df_final = processar_arquivos_ssw(diretorio)

#     if not df_final.empty:
#         caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
#         df_final.to_excel(caminho_saida, index=False)
#         print(f"Arquivo Excel salvo em: {caminho_saida}")
#     else:
#         print("Nenhum resultado para salvar.")
