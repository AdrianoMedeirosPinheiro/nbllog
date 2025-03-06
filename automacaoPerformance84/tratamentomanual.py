import pandas as pd
import re
import os
from io import StringIO

def ler_arquivo_ssw(caminho_arquivo):
    """
    Lê um arquivo .sswweb específico (passando o caminho completo) e retorna um DataFrame.
    Remove tudo antes da linha com '-------------------------------------------------------'
    e valores não numéricos na segunda coluna.
    """
    try:
        print(f"[DEBUG] Lendo arquivo: {caminho_arquivo}")
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        # Encontrar o índice da linha com o marcador '-------------------------------------------------------'
        inicio_dados = next((i for i, linha in enumerate(linhas) 
                             if '-------------------------------------------------------' in linha), None)
        if inicio_dados is None:
            print("[ERROR] Marcador '-------------------------------------------------------' não encontrado.")
            return pd.DataFrame()

        # Remover todas as linhas antes do marcador
        linhas = linhas[inicio_dados + 1:]

        # Definir colspecs para o arquivo com base no layout
        colspecs = [
            (0, 55), (55, 62), (62, 71), (71, 82), (82, 88),
            (88, 96), (96, 102), (102, 112), (112, 120), (120, 129),
            (129, 136), (136, 147), (147, 154), (154, 160), (160, 170), (170, 177)
        ]

        # Criar o DataFrame
        df = pd.read_fwf(StringIO("".join(linhas)), colspecs=colspecs, header=None)
        df.columns = [
            "UNIDADE_DESTINO", "QUANTIDADE_CLIENTES", "QCTRCS_EXPEDIDOS", "ENTREGUE_QCTRC",
            "ENTREGUE_%", "NOPRAZO_QCTRC", "NOPRAZO_%", "ATRASCLI_QCTRC", "ATRASCLI_%",
            "ATRASTRANSP_QCTRC", "ATRASTRANSP_%", "PERF_%", "NOPRAZO_TOTAL_QCTRC",
            "NOPRAZO_TOTAL_%", "ATRASADO_QCTRC", "ATRASADO_%"
        ]

        # Remover linhas não numéricas na coluna 'QUANTIDADE_CLIENTES'
        df = df[df["QUANTIDADE_CLIENTES"].apply(lambda x: str(x).replace('.', '', 1).isdigit())]

        # Remover linhas repetitivas que correspondem ao cabeçalho (mantendo as iniciais)
        df = df[~df["UNIDADE_DESTINO"].astype(str).str.contains("UNIDADE DESTINO", na=False)]
        df.reset_index(drop=True, inplace=True)

        return df

    except Exception as e:
        print(f"[ERROR] Erro ao processar o arquivo {caminho_arquivo}: {e}")
        return pd.DataFrame()

def preencher_unidade_destino(df):
    """
    Preenche os valores NaN na coluna 'UNIDADE_DESTINO' com o último valor não nulo.
    """
    try:
        df['UNIDADE_DESTINO'] = df['UNIDADE_DESTINO'].ffill()
        return df
    except Exception as e:
        print(f"[ERROR] Erro ao preencher 'UNIDADE_DESTINO': {e}")
        return df

def ler_linhas_ignoradas(caminho_arquivo):
    """
    Lê o arquivo SSW e retorna as primeiras 5 linhas ignoradas,
    além de extrair a data, hora e mês do relatório.
    """
    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        linhas_ignoradas = linhas[:5]
        df_ignoradas = pd.DataFrame(linhas_ignoradas, columns=['LINHA_IGNORADA'])

        # Captura DATA_DO_RELATORIO e HORA_DO_RELATORIO
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

        # Captura MES_DO_RELATORIO
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
    Extrai (quantidade, percentual) de uma string no formato "XXX YYY%"
    """
    try:
        if not isinstance(valor, str) or valor.lower().strip() == "nan":
            return 0, 0
        # Normaliza
        valor = valor.replace(',', '.').replace('%', '').strip()
        partes = valor.split()
        if len(partes) == 2:
            quantidade, percentual = partes
        elif len(partes) == 1:
            # Se só existe 1 valor, podemos assumir que é o percentual
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
    Lê TODOS os arquivos .sswweb em um diretório, processa e unifica em um único DataFrame.
    """
    # Lista todos os arquivos .sswweb no diretório
    arquivos = [f for f in os.listdir(diretorio) if f.lower().endswith('.sswweb')]
    
    if not arquivos:
        print("[ERROR] Nenhum arquivo .sswweb encontrado no diretório.")
        return pd.DataFrame()

    df_list = []
    
    for arquivo in arquivos:
        caminho_arquivo = os.path.join(diretorio, arquivo)
        print(f"[DEBUG] Processando arquivo: {caminho_arquivo}")

        # ----------------------------
        # 1) Ler conteúdo principal
        # ----------------------------
        df = ler_arquivo_ssw(caminho_arquivo)
        if df.empty:
            print(f"[WARNING] DataFrame vazio para o arquivo {arquivo}. Pulando...")
            continue  # Pula para o próximo arquivo

        # ----------------------------
        # 2) Tratar colunas de % (ENTREGUE_%, NOPRAZO_%, etc.)
        # ----------------------------
        colunas_para_separar = ['ENTREGUE_%', 'NOPRAZO_%', 'ATRASCLI_%', 'ATRASTRANSP_%']
        for coluna in colunas_para_separar:
            if coluna in df.columns:
                try:
                    df[f'Quantidade_{coluna.strip("_")}'], df[f'Percentual_{coluna.strip("_")}'] = zip(
                        *df[coluna].apply(separar_valores)
                    )
                except Exception as e:
                    print(f"[ERROR] Erro ao processar a coluna '{coluna}' em {arquivo}: {e}")

        # ----------------------------
        # 3) Preencher UNIDADE_DESTINO
        # ----------------------------
        if 'UNIDADE_DESTINO' in df.columns:
            df = preencher_unidade_destino(df)
        else:
            print("[ERROR] Coluna 'UNIDADE_DESTINO' não encontrada para preenchimento.")

        # ----------------------------
        # 4) Remover pontos (.) das colunas numéricas
        # ----------------------------
        # Começando da segunda coluna (índice 1) que não terminam com '%'
        cols_to_clean = [col for col in df.columns[1:] if not col.endswith('%')]
        for col in cols_to_clean:
            df[col] = df[col].astype(str)
            df[col] = df[col].str.replace('.', '', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # ----------------------------
        # 5) Ler linhas ignoradas (data/hora/mês)
        # ----------------------------
        df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO = ler_linhas_ignoradas(caminho_arquivo)
        df['DATA_DO_RELATORIO'] = DATA_DO_RELATORIO
        df['HORA_DO_RELATORIO'] = HORA_DO_RELATORIO
        df['MES_DO_RELATORIO'] = MES_DO_RELATORIO

        # ----------------------------
        # 6) Excluir colunas desnecessárias
        # ----------------------------
        # (conforme no seu código, remove as colunas [16:24], se existirem)
        if len(df.columns) > 16:
            df.drop(df.columns[16:24], axis=1, inplace=True, errors='ignore')

        # Adiciona ao df_list
        df_list.append(df)

    # Concatena todos os DataFrames em um único
    if df_list:
        df_final = pd.concat(df_list, ignore_index=True)
        print("[DEBUG] Todos os arquivos foram processados com sucesso.")
        return df_final
    else:
        print("[WARNING] Não foi possível criar nenhum DataFrame unificado.")
        return pd.DataFrame()

# Exemplo de uso:
if __name__ == "__main__":
    diretorio = r'C:\Users\NBL\Desktop\Automacoes NBL\automacao - Performance84\base'
    df_unificado = processar_arquivos_ssw(diretorio)
    
    if not df_unificado.empty:
        caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
        df_unificado.to_excel(caminho_saida, index=False)
        print(f"[DEBUG] Arquivo Excel salvo em: {caminho_saida}")
    else:
        print("[DEBUG] Nenhum DataFrame final para salvar.")
