from datetime import datetime

def filtrar_ultimas_linhas(rows, criterios_opcao, criterios_usuario, config_filtrar_todos=False):
    """
    Filtra as linhas da tabela que atendem aos critérios de 'opcao' e 'usuario'.
    
    Parâmetros:
    - rows: Lista de linhas da tabela.
    - criterios_opcao: Critério esperado para a coluna 'opcao'.
    - criterios_usuario: Critério esperado para a coluna 'usuario'.
    - config_filtrar_todos: Se True, filtra as linhas dos últimos dois anos; se False, filtra as 3 últimas linhas.
    
    Retorna:
    - Uma lista com as linhas que atendem aos critérios, incluindo o índice, o elemento da linha, e a data/hora de solicitação.
    """
    filtered_rows = []
    
    for index, row in enumerate(rows, start=1):  # `start=1` para começar a contagem do cabeçalho como linha 1
        # Extrai o texto de cada célula da linha
        cells = row.query_selector_all('td')
        if len(cells) < 5:
            continue  # Pula linhas que não têm o número esperado de colunas

        # Extrai as colunas desejadas
        opcao = cells[1].inner_text().strip()
        usuario = cells[3].inner_text().strip()
        data_hora_solicitacao = cells[2].inner_text().strip()
        situacao = cells[6].inner_text().strip()

        # Verifica se a linha corresponde aos critérios
        if opcao == criterios_opcao and usuario == criterios_usuario:
            # Armazena o índice da linha junto com os dados
            filtered_rows.append((index, row, data_hora_solicitacao))  

    # Ordena as linhas filtradas pela coluna de Data/Hora Solicitação (do mais recente para o mais antigo)
    filtered_rows.sort(key=lambda x: x[2], reverse=True)

    # Filtragem baseada na configuração
    if config_filtrar_todos:
        # Filtrar apenas as linhas do ano atual e do ano anterior
        ano_atual = datetime.today().year
        ano_anterior = ano_atual - 1
        filtered_rows = [
            row for row in filtered_rows 
            if int(row[2].split('/')[-1]) in {ano_atual, ano_anterior}
        ]
    else:
        # Filtrar apenas as 9 últimas linhas
        filtered_rows = filtered_rows[:9]

    return filtered_rows
