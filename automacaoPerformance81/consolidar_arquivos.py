import pandas as pd
import re
import os
from io import StringIO

def ler_arquivo_ssw(diretorio):
    """
    Lê o primeiro arquivo .sswweb encontrado no diretório especificado e retorna um DataFrame.
    
    Parâmetros:
        diretorio (str): Caminho para o diretório onde o arquivo .sswweb está localizado.
        
    Retorna:
        tuple: (DataFrame, caminho_arquivo) ou (DataFrame vazio, None) em caso de erro.
    """
    try:
        # Buscar arquivos com a extensão .sswweb no diretório especificado
        arquivos = [f for f in os.listdir(diretorio) if f.lower().endswith('.sswweb')]
        
        if not arquivos:
            print("ERRO: Nenhum arquivo .sswweb encontrado no diretório.")
            return pd.DataFrame(), None

        # Selecionar o primeiro arquivo encontrado
        caminho_arquivo = os.path.join(diretorio, arquivos[0])
        print(f"Arquivo encontrado: {caminho_arquivo}")

        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()
        
        # Identificar a linha de cabeçalho que contém "UNIDADE DESTINO"
        cabecalho_index = None
        for i, linha in enumerate(linhas):
            if "UNIDADE DESTINO" in linha.upper():
                cabecalho_index = i
                break
        
        if cabecalho_index is None:
            print("ERRO: Linha de cabeçalho com 'UNIDADE DESTINO' não encontrada.")
            return pd.DataFrame(), None
        
        # Extrair as linhas a partir da linha de cabeçalho
        dados_linhas = linhas[cabecalho_index:]
        
        # Determinar as posições de início das colunas com base no cabeçalho
        cabecalho = dados_linhas[0]
        posicoes = [m.start() for m in re.finditer(r'\S+', cabecalho)]
        colspecs = [(posicoes[i], posicoes[i+1]) for i in range(len(posicoes)-1)]
        colspecs.append((posicoes[-1], None))  # Última coluna até o fim da linha
        
        # Ler o conteúdo utilizando colspecs
        df = pd.read_fwf(StringIO("".join(dados_linhas)), colspecs=colspecs)
        
        # Definir os nomes das colunas com base no cabeçalho
        nomes_colunas = re.findall(r'\S+', cabecalho)
        df.columns = nomes_colunas[:len(df.columns)]
        
        # Remover possíveis linhas de cabeçalho duplicadas nos dados
        df = df[~df[nomes_colunas[0]].astype(str).str.contains("UNIDADE DESTINO", na=False)]
        
        # Resetar o índice do DataFrame
        df.reset_index(drop=True, inplace=True)
        
        # Verificar as colunas após a leitura para diagnóstico
        print("Colunas após a leitura do arquivo:", df.columns)
        
        # Renomear as primeiras duas colunas
        if len(df.columns) >= 2:
            df = df.rename(columns={df.columns[0]: 'CIDADE_DESTINO', df.columns[1]: 'DESTINO_Prazo'})
            print("Colunas renomeadas para 'CIDADE_DESTINO' e 'DESTINO_Prazo'.")
        else:
            print("ERRO: Menos de 2 colunas encontradas no DataFrame.")
            return pd.DataFrame(), None

        # Verificar as primeiras linhas para garantir que a renomeação foi aplicada corretamente
        print("Primeiras linhas do DataFrame após renomeação:")
        print(df.head())
        
        # Aplicar modificações a partir da quinta linha (inclusive)
        if len(df) > 4:
            # Separar o DataFrame em cabeçalho (linhas 0 a 4) e dados (a partir da linha 5)
            headers = df.iloc[:5].copy()
            data = df.iloc[5:].copy()
            
            # Verificar se as colunas 'CIDADE_DESTINO' e 'DESTINO_Prazo' existem
            if 'CIDADE_DESTINO' not in data.columns or 'DESTINO_Prazo' not in data.columns:
                print("ERRO: Colunas 'CIDADE_DESTINO' e/ou 'DESTINO_Prazo' não encontradas no DataFrame de dados.")
                return pd.DataFrame(), None
            
            # Função para processar 'CIDADE_DESTINO' e 'DESTINO_Prazo'
            def processar_cidade_destino_prazo(row):
                destino_prazo = row['DESTINO_Prazo']
                
                if pd.isna(destino_prazo):
                    return pd.Series({'CIDADE_DESTINO': row['CIDADE_DESTINO'], 'PRAzo': pd.NA})
                
                destino_prazo_str = str(destino_prazo).strip()
                match = re.search(r'(\d+)', destino_prazo_str)
                
                if match:
                    prazo = int(match.group(1))
                    texto_extra = destino_prazo_str[:match.start()].strip()
                    cidade_destino_atualizado = f"{row['CIDADE_DESTINO']} {texto_extra}".strip()
                    return pd.Series({'CIDADE_DESTINO': cidade_destino_atualizado, 'PRAzo': prazo})
                else:
                    cidade_destino_atualizado = f"{row['CIDADE_DESTINO']} {destino_prazo_str}".strip()
                    return pd.Series({'CIDADE_DESTINO': cidade_destino_atualizado, 'PRAzo': pd.NA})
            
            data[['CIDADE_DESTINO', 'PRAzo']] = data.apply(processar_cidade_destino_prazo, axis=1)
            df_final = pd.concat([headers, data], ignore_index=True)
        else:
            df_final = df.copy()
            print("Aviso: O DataFrame tem 5 linhas ou menos; nenhuma modificação adicional foi feita.")
        
        print(f"Arquivo processado com sucesso: {caminho_arquivo}")
        return df_final, caminho_arquivo
    
    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return pd.DataFrame(), None

def manipular_cidade_destino(df):
    """
    Processa as colunas 'CIDADE_DESTINO' e 'PRAzo' para separar em 'CIDADE' e 'DESTINO_PRAZO'.
    
    Parâmetros:
        df (DataFrame): DataFrame com as colunas a serem processadas.
        
    Retorna:
        DataFrame: DataFrame com as colunas manipuladas.
    """
    try:
        # Remover espaços extras e normalizar os nomes das colunas
        df.columns = df.columns.str.strip().str.replace('|', '_').str.replace(' ', '_')
    
        # Verificar se as colunas 'CIDADE_DESTINO' e 'PRAzo' existem
        if 'CIDADE_DESTINO' in df.columns and 'PRAzo' in df.columns:
            # Função para processar cada linha
            def processar_linha(row):
                destino_prazo = str(row['PRAzo']).strip()
                
                # Extrair números de 'PRAzo'
                numero = ''.join(re.findall(r'\d+', destino_prazo))
                
                # Extrair texto (remover os números) para adicionar em 'CIDADE_DESTINO'
                texto = re.sub(r'\d+', '', destino_prazo).strip()
                
                # Atualizar 'CIDADE_DESTINO' com o texto da coluna 'PRAzo'
                cidade_atualizada = f"{row['CIDADE_DESTINO']} {texto}".strip()
                
                # Atualizar 'PRAzo' com apenas o número
                return pd.Series({'CIDADE': cidade_atualizada, 'DESTINO_PRAZO': numero if numero else pd.NA})
            
            # Aplicar a função a cada linha do DataFrame
            df[['CIDADE', 'DESTINO_PRAZO']] = df.apply(processar_linha, axis=1)
            print("Colunas 'CIDADE_DESTINO' e 'PRAzo' manipuladas com sucesso.")
        else:
            print("ERRO: As colunas 'CIDADE_DESTINO' e/ou 'PRAzo' não foram encontradas no DataFrame.")
        
        return df
    
    except Exception as e:
        print(f"Erro ao manipular 'CIDADE_DESTINO' e 'PRAzo': {e}")
        return df

def separar_valores(valor):
    """
    Separa uma string em quantidade e percentual.
    
    Parâmetros:
        valor (str): String contendo quantidade e percentual separados por espaço.
        
    Retorna:
        tuple: (quantidade, percentual) como inteiros e floats, respectivamente.
    """
    try:
        if isinstance(valor, str) and valor.lower() != "nan":
            valor_split = valor.split()  # Separa pelo espaço
            if len(valor_split) == 2:
                quantidade = valor_split[0].replace('.', '').replace(',', '').replace('-', '0')  # Remove pontos e vírgulas
                percentual = valor_split[1].replace(',', '.').replace('-', '0')
                quantidade = int(quantidade) if quantidade.isdigit() else 0
                percentual = float(percentual) if re.match(r'^-?\d+(\.\d+)?$', percentual) else 0.0
                return quantidade, percentual
        return 0, 0  # Se o formato for incorreto, retorna 0,0
    except Exception as e:
        print(f"Erro ao processar valor: {valor}. Erro: {e}")
        return 0, 0

def ler_linhas_ignoradas(caminho_arquivo):
    """
    Lê um arquivo SSW e retorna as primeiras 5 linhas ignoradas do arquivo,
    além de extrair a data, hora e mês do relatório.
    
    Parâmetros:
        caminho_arquivo (str): Caminho completo para o arquivo .sswweb.
        
    Retorna:
        tuple: (DataFrame das linhas ignoradas, data do relatório, hora do relatório, mês do relatório)
    """
    try:
        with open(caminho_arquivo, 'r', encoding='latin1') as file:
            linhas = file.readlines()

        # Ignorar as primeiras 5 linhas e criar um DataFrame com elas
        linhas_ignoradas = linhas[:5]

        # Criar DataFrame com as linhas ignoradas
        df_ignoradas = pd.DataFrame(linhas_ignoradas, columns=['LINHA_IGNORADA'])

        # Extração da Data e Hora da segunda linha (index 1)
        if len(linhas) > 1:
            segunda_linha = linhas[1]
            match_data_hora = re.search(r'(\d{2}/\d{2}/\d{2}) (\d{2}:\d{2})', segunda_linha)
            
            if match_data_hora:
                DATA_DO_RELATORIO = match_data_hora.group(1)  # Ex: 01/12/24
                HORA_DO_RELATORIO = match_data_hora.group(2)  # Ex: 01:47
            else:
                DATA_DO_RELATORIO = None
                HORA_DO_RELATORIO = None
        else:
            DATA_DO_RELATORIO = None
            HORA_DO_RELATORIO = None

        # Extração do mês da terceira linha (index 2)
        if len(linhas) > 2:
            terceira_linha = linhas[2]
            match_mes = re.search(r'MES: (\d{2}/\d{2})', terceira_linha)
            
            if match_mes:
                MES_DO_RELATORIO = match_mes.group(1)  # Ex: 11/24
            else:
                MES_DO_RELATORIO = None
        else:
            MES_DO_RELATORIO = None

        # Exibindo as variáveis extraídas
        print(f"Data do Relatório: {DATA_DO_RELATORIO}")
        print(f"Hora do Relatório: {HORA_DO_RELATORIO}")
        print(f"Mês do Relatório: {MES_DO_RELATORIO}")

        return df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO

    except Exception as e:
        print(f"Erro ao processar o arquivo para linhas ignoradas: {e}")
        return pd.DataFrame(), None, None, None

def preencher_unidade_destino(df):
    """
    Preenche os valores NaN na coluna 'UNIDADE_DESTINO' com o último valor não-nulo.
    
    Parâmetros:
        df (DataFrame): DataFrame contendo a coluna 'UNIDADE_DESTINO'.
        
    Retorna:
        DataFrame: DataFrame com a coluna 'UNIDADE_DESTINO' preenchida.
    """
    try:
        df['UNIDADE_DESTINO'] = df['UNIDADE_DESTINO'].ffill()
        print("Coluna 'UNIDADE_DESTINO' preenchida com valores anteriores.")
        return df
    except Exception as e:
        print(f"Erro ao preencher 'UNIDADE_DESTINO': {e}")
        return df

def main():
    # Defina o diretório onde os arquivos .sswweb estão localizados
    diretorio = r'C:\Users\NBL\Desktop\Automacoes NBL\automacao - Performance\base'  # Substitua pelo diretório desejado
    
    # Ler o arquivo .sswweb e obter o DataFrame e o caminho do arquivo
    df, caminho_arquivo = ler_arquivo_ssw(diretorio)
    
    if df.empty or caminho_arquivo is None:
        print("Processo interrompido devido a erros na leitura do arquivo.")
        return
    
    # Remover espaços extras e normalizar os nomes das colunas
    df.columns = df.columns.str.strip().str.replace('|', '_').str.replace(' ', '_')
    
    # Concatenar as colunas 'CIDADE_DESTINO' e 'DESTINO_Prazo', separando com um espaço
    if 'CIDADE_DESTINO' in df.columns and 'DESTINO_Prazo' in df.columns:
        df['UNIDADE_DESTINO'] = df['CIDADE_DESTINO'].fillna('') + ' ' + df['DESTINO_Prazo'].fillna('')
    else:
        print("ERRO: Colunas 'CIDADE_DESTINO' e/ou 'DESTINO_Prazo' não encontradas para concatenar.")
        return
    
    # Remover as colunas originais
    df = df.drop(['CIDADE_DESTINO', 'DESTINO_Prazo'], axis=1, errors='ignore')
    
    # Manipular as colunas 'CIDADE' e 'DESTINO_PRAZO'
    df = manipular_cidade_destino(df)
    
    # Aplicar a função para separar os valores de cada coluna
    colunas_para_separar = ['ENTREGUE', '_NOPRAZO', '_ATRASCLI', '_ATRASTRANSP', 'NOPRAZO', '_ATRASADO']
    for coluna in colunas_para_separar:
        if coluna in df.columns:
            df[f'Quantidade_{coluna.strip("_")}'], df[f'Percentual_{coluna.strip("_")}'] = zip(*df[coluna].apply(separar_valores))
        else:
            print(f"Aviso: Coluna '{coluna}' não encontrada no DataFrame.")
    
    # Verifique os valores problemáticos para diagnóstico
    padrao = r'^\d+[\.,]?\d*\s+\d+[\.,]?\d*$'
    colunas_para_verificar = ['ENTREGUE', '_NOPRAZO', '_ATRASCLI', '_ATRASTRANSP', 'NOPRAZO', '_ATRASADO']
    for coluna in colunas_para_verificar:
        if coluna in df.columns:
            problemas = df[~df[coluna].astype(str).str.match(padrao, na=False)]
            if not problemas.empty:
                print(f"\nValores problemáticos encontrados na coluna '{coluna}':")
                print(problemas)
        else:
            print(f"Aviso: Coluna '{coluna}' não encontrada para verificação de valores problemáticos.")
    
    # Converta as colunas para tipos numéricos válidos, considerando valores inválidos
    colunas_numericas = [col for col in df.columns if col.startswith('Quantidade_') or col.startswith('Percentual_')]
    for coluna in colunas_numericas:
        if coluna in df.columns:
            if 'Quantidade_' in coluna:
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0).astype(int)
            elif 'Percentual_' in coluna:
                df[coluna] = pd.to_numeric(df[coluna], errors='coerce').fillna(0.0).astype(float)
        else:
            print(f"Aviso: Coluna '{coluna}' não encontrada para conversão de tipo.")
    
    # Preencher os valores NaN na coluna 'UNIDADE_DESTINO' com o último valor não-nulo
    if 'UNIDADE_DESTINO' in df.columns:
        df = preencher_unidade_destino(df)
    else:
        print("ERRO: Coluna 'UNIDADE_DESTINO' não encontrada para preenchimento.")
    
    # Ler as linhas ignoradas e extrair informações de data/hora
    df_ignoradas, DATA_DO_RELATORIO, HORA_DO_RELATORIO, MES_DO_RELATORIO = ler_linhas_ignoradas(caminho_arquivo)
    
    # Adicionar as informações de data, hora e mês ao DataFrame principal
    df['DATA_DO_RELATORIO'] = DATA_DO_RELATORIO
    df['HORA_DO_RELATORIO'] = HORA_DO_RELATORIO
    df['MES_DO_RELATORIO'] = MES_DO_RELATORIO
    
    # Exibir o DataFrame final
    print("\nDataFrame Final:")
    print(df.head())
    
    # Opcional: Salvar o DataFrame final em um arquivo Excel ou CSV
    caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
    df.to_excel(caminho_saida, index=False)
    print(f"\nDataFrame salvo em: {caminho_saida}")

if __name__ == "__main__":
    main()
