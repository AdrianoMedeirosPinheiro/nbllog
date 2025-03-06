def carregar_e_filtrar_tabela(new_page, tabela_selector='#tblsr'):
    """
    Aguarda o carregamento da tabela, seleciona todas as linhas e define critérios para filtragem.
    
    Parâmetros:
    - new_page: Página atual do Playwright onde a tabela será carregada.
    - tabela_selector: Seletor da tabela para carregar as linhas (padrão: '#tblsr').
    
    Retorna:
    - rows: Lista de linhas encontradas na tabela.
    - criterios_usuario: Critério definido para a coluna 'usuario'.
    - criterios_opcao: Critério definido para a coluna 'opcao'.
    - criterios_situacao: Critério definido para a coluna 'situacao'.
    """
    # Aguarda a tabela carregar na nova página
    new_page.wait_for_selector(tabela_selector)
    
    # Seleciona todas as linhas dentro da tabela especificada
    rows = new_page.query_selector_all(f'{tabela_selector} tr')
    
    # Imprime a quantidade de linhas encontradas
    print(f"Total de linhas encontradas: {len(rows)}")
    
    # Exibe o conteúdo de cada linha (opcional)
    for row in rows:
        print(row.inner_text())
    
    # Define os critérios para as linhas que deseja baixar
    criterios_usuario = 'danilo'
    criterios_opcao = '455 - Fretes Expedidos/Recebidos - CTRCs'
    criterios_situacao = 'Concluído'
    
    return rows, criterios_usuario, criterios_opcao, criterios_situacao

# rows, criterios_usuario, criterios_opcao, criterios_situacao = carregar_e_filtrar_tabela(new_page)
