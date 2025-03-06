def filtrar_ultimas_linhas(rows, criterios_opcao, criterios_usuario):
    """
    Filtra as 10 linhas mais recentes da tabela que atendem aos critérios de 'opcao' e 'usuario'.
    
    Parâmetros:
    - rows: Lista de linhas da tabela.
    - criterios_opcao: Critério esperado para a coluna 'opcao'.
    - criterios_usuario: Critério esperado para a coluna 'usuario'.
    
    Retorna:
    - Uma lista com as 10 últimas linhas que atendem aos critérios, incluindo o índice, o elemento da linha, e a data/hora de solicitação.
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
            filtered_rows.append((index, row, data_hora_solicitacao))  # Armazena o índice da linha junto com os dados

    # Ordena as linhas filtradas pela coluna de Data/Hora Solicitação (do mais recente para o mais antigo)
    filtered_rows.sort(key=lambda x: x[2], reverse=True)

    # Seleciona as 10 linhas mais recentes que atendem aos critérios
    return filtered_rows[:13]

# filtered_rows = filtrar_ultimas_linhas(rows, criterios_opcao, criterios_usuario)
