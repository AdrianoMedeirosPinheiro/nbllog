import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
from openpyxl import Workbook

def processar_arquivo_sswweb(caminho_arquivo, chunk_size=10000):
    """
    Lê um arquivo .sswweb em chunks e retorna um DataFrame.
    """
    dfs = []
    try:
        for chunk in pd.read_csv(caminho_arquivo, delimiter=';', skiprows=1, encoding='latin1', chunksize=chunk_size):
            dfs.append(chunk)
    except Exception as e:
        print(f"Erro ao ler o arquivo {caminho_arquivo}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def consolidar_arquivos_sswweb_para_xlsx(diretorio, arquivo_saida):
    """
    Processa todos os arquivos .sswweb em um diretório, consolida e exporta diretamente para um arquivo .xlsx.
    
    Parâmetros:
    - diretorio: Caminho para o diretório contendo os arquivos .sswweb.
    - arquivo_saida: Caminho completo do arquivo .xlsx de saída.
    """
    # Cria o arquivo Excel e a primeira aba
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Relatorios Consolidado"

    # Encontra todos os arquivos .sswweb no diretório especificado
    arquivos_sswweb = [os.path.join(diretorio, f) for f in os.listdir(diretorio) if f.endswith('.sswweb')]

    # Processa os arquivos em paralelo para maior desempenho
    with ProcessPoolExecutor() as executor:
        resultados = list(executor.map(processar_arquivo_sswweb, arquivos_sswweb))

    # Adiciona o cabeçalho ao Excel, usando o cabeçalho do primeiro DataFrame
    if resultados:
        header = list(resultados[0].columns)  # Extrai e transforma o cabeçalho em lista
        worksheet.append(header)

    # Escreve cada DataFrame consolidado no Excel, linha por linha
    for df in resultados:
        for row in df.itertuples(index=False, name=None):
            worksheet.append(row)
    
    # Salva o arquivo Excel consolidado
    workbook.save(arquivo_saida)
    print(f"Arquivo consolidado salvo em {arquivo_saida}")

# Exemplo de chamada da função
# diretorio = 'C:\\automacao\\base'  # Diretório com os arquivos .sswweb
# arquivo_saida = 'C:\\automacao\\base\\relatorios_consolidados.xlsx'
# consolidar_arquivos_sswweb_para_xlsx(diretorio, arquivo_saida)
