import pandas as pd
import re
import os
from io import StringIO

def ler_arquivo_ssw(diretorio):
    """
    Lê o primeiro arquivo .sswweb encontrado no diretório especificado.
    Retorna um DataFrame com base nos colspecs definidos, sem excluir linhas ou converter colunas.
    """
    try:
        arquivos = [f for f in os.listdir(diretorio) if f.lower().endswith('.sswweb')]
        if not arquivos:
            print("ERRO: Nenhum arquivo .sswweb encontrado no diretório.")
            return pd.DataFrame(), None

        caminho_arquivo = os.path.join(diretorio, arquivos[0])
        print(f"Arquivo encontrado: {caminho_arquivo}")

        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            conteudo = file.read()

        colspecs = [
            (0, 15),    # CTRC
            (15, 23),   # NF
            (23, 44),   # REMETENTE
            (44, 62),   # PAGADOR/ABC
            (64, 86),   # DESTINATARIO
            (86, 103),  # CIDADE DESTINO
            (103, 112), # PREVENTR
            (112, 120), # NOPRAZO
            (120, 128), # ATRASADO
            (128, 132), # --
            (132, 136), # -5
            (136, 140), # -4
            (140, 144), # -3
            (144, 148), # -2
            (148, 152), # -1
            (152, 156), # 0
            (156, 160), # +1
            (160, 164), # +2
            (164, 168), # +3
            (168, 172), # +4
            (172, 176), # +5
            (176, 180), # +6
            (180, 184), # +7
            (184, 188), # +8
            (188, 192)  # +9
        ]

        df = pd.read_fwf(StringIO(conteudo), colspecs=colspecs, header=None)
        df.columns = [
            "CTRC", "NF", "REMETENTE", "PAGADOR/ABC", "DESTINATARIO", "CIDADE DESTINO",
            "PREVENTR", "NOPRAZO", "ATRASADO", "--", "-5", "-4", "-3", "-2", "-1", "0",
            "+1", "+2", "+3", "+4", "+5", "+6", "+7", "+8", "+9"
        ]

        return df, caminho_arquivo
    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return pd.DataFrame(), None

def ler_linhas_ignoradas(caminho_arquivo):
    """
    Lê as primeiras 5 linhas do arquivo e extrai datas e horários.
    """
    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        df_ignoradas = pd.DataFrame(linhas[:5], columns=['LINHA_IGNORADA'])

        DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO, DATA_COMPETENCIA = None, None, None, None

        if len(linhas) > 2:
            terceira_linha = linhas[2]
            match_data_hora = re.search(r'(\d{2}/\d{2}/\d{2,4})(?:\s+(\d{2}:\d{2}))?', terceira_linha)
            if match_data_hora:
                DATA_DO_RELATORIO = match_data_hora.group(1)
                if match_data_hora.group(2):
                    HORA_DO_RELATORIO = match_data_hora.group(2)

            match_mes = re.search(r'MES:\s*(\d{2}/\d{2})', terceira_linha)
            if match_mes:
                MES_DO_RELATORIO = match_mes.group(1)

        if len(linhas) > 3:
            quarta_linha = linhas[3]
            match_data_comp = re.search(r'(\d{2}/\d{2}/\d{2,4})', quarta_linha)
            if match_data_comp:
                DATA_COMPETENCIA = match_data_comp.group(1)

        return df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO, DATA_COMPETENCIA

    except Exception as e:
        print(f"Erro ao processar as linhas iniciais: {e}")
        return pd.DataFrame(), None, None, None, None

def processar_arquivo_ssw(diretorio):
    """
    1) Lê o arquivo e gera um DataFrame.
    2) Preenche a coluna UNIDADE em blocos, entre cada 'FILIAL DESTINO:'.
    3) Remove as linhas cujo CTRC não segue o padrão 3 letras + espaço + 6 números + hífen + 1 dígito.
    4) Cria a coluna RESPONSABILIDADE CLIENTE:
       - "Sim" se 'ATRASADO' tiver '*'
       - "Não" caso contrário
    5) Remove o '*' da coluna ATRASADO.
    """
    df, caminho_arquivo = ler_arquivo_ssw(diretorio)
    if df.empty or not caminho_arquivo:
        print("Processo interrompido (sem arquivo ou DataFrame vazio).")
        return pd.DataFrame()

    # Extrai possíveis datas
    df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO, DATA_COMPETENCIA = ler_linhas_ignoradas(caminho_arquivo)

    df['DATA_DO_RELATORIO'] = DATA_DO_RELATORIO
    df['HORA_DO_RELATORIO'] = HORA_DO_RELATORIO
    df['MES_DO_RELATORIO'] = MES_DO_RELATORIO
    if DATA_COMPETENCIA:
        df['DATA_COMPETENCIA'] = DATA_COMPETENCIA

    # Cria a coluna UNIDADE
    df['UNIDADE'] = None

    # Regex para identificar CTRC (ex. 'BES 447752-9')
    padrao_ctrc = re.compile(r'^[A-Z]{3}\s\d{6}-\d$')

    # Localiza as linhas onde CTRC inicia com 'FILIAL DESTINO:'
    linhas_filial = df.index[df['CTRC'].str.startswith('FILIAL DESTINO:', na=False)].tolist()

    # Adiciona um marco no fim do DF
    linhas_filial.append(len(df))

    # Percorre blocos
    for i in range(len(linhas_filial) - 1):
        linha_inicio = linhas_filial[i]
        linha_fim = linhas_filial[i + 1]  # não incluso

        # Pega 3 primeiras letras da coluna NF na linha de 'FILIAL DESTINO:'
        sigla = None
        nf_val = df.at[linha_inicio, 'NF']
        if isinstance(nf_val, str) and nf_val.strip():
            sigla = nf_val[:3]

        if not sigla:
            continue

        # Preenche 'UNIDADE' nos CTRCs do bloco
        bloco_indices = df.index[(df.index >= (linha_inicio + 1)) & (df.index < linha_fim)]
        for idx in bloco_indices:
            valor_ctrc = str(df.at[idx, 'CTRC']).strip()
            if padrao_ctrc.match(valor_ctrc):
                df.at[idx, 'UNIDADE'] = sigla

    # === Passo 3: manter apenas quem segue o padrão na coluna CTRC ===
    df = df[df['CTRC'].str.match(padrao_ctrc, na=False)].copy()

    # === Passo 4: criar coluna RESPONSABILIDADE CLIENTE com base em ATRASADO
    # "Sim" se contiver '*', caso contrário "Não".
    df['ATRASADO'] = df['ATRASADO'].fillna('').astype(str)
    df['RESPONSABILIDADE CLIENTE'] = df['ATRASADO'].apply(lambda x: "Sim" if "*" in x else "Não")

    # === Passo 5: remover '*' da coluna ATRASADO
    df['ATRASADO'] = df['ATRASADO'].str.replace('*', '', regex=False)

    return df

# # ===============================
# # Exemplo de chamada principal
# # ===============================
# if __name__ == "__main__":
#     diretorio = r"C:\\Users\\NBL\\Desktop\\Automacoes NBL\\automacao - PerformanceDiariaRelatorio\\base"
#     df_final = processar_arquivo_ssw(diretorio)

#     if not df_final.empty:
#         saida = os.path.join(diretorio, "resultado_processado.xlsx")
#         df_final.to_excel(saida, index=False)
#         print(f"Arquivo Excel salvo em: {saida}")
