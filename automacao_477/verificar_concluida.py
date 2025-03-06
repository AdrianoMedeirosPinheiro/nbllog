import time
from datetime import datetime

def verificar_situacao_concluida(new_page, criterios_opcao, criterios_usuario, config_verificar_todos=False, update_button_selector='#\\32', tabela_selector='#tblsr > tbody > tr'):
    """
    Função para verificar se as linhas mais recentes da tabela têm a situação "Concluído".
    
    Parâmetros:
    - new_page: Página atual do Playwright onde a tabela está sendo verificada.
    - criterios_opcao: Critério esperado para a coluna 'opcao'.
    - criterios_usuario: Critério esperado para a coluna 'usuario'.
    - config_verificar_todos: Se True, verifica todas as linhas do ano atual e do ano anterior; se False, verifica apenas as 3 últimas linhas.
    - update_button_selector: Seletor do botão que atualiza a tabela (padrão: '#\\32').
    - tabela_selector: Seletor da tabela onde as linhas serão verificadas (padrão: '#tblsr > tbody > tr').
    """

    while True:
        todas_concluidas = True  # Assumimos inicialmente que todas estão concluídas

        # Recarrega todas as linhas da tabela para garantir que novas entradas sejam incluídas
        rows = new_page.query_selector_all(tabela_selector)  # Seleciona todas as linhas da tabela novamente
        todas_linhas = []

        # Extrai as informações de cada linha para posterior filtragem e ordenação
        for index, row in enumerate(rows, start=1):
            cells = row.query_selector_all('td')
            if len(cells) < 5:
                continue  # Ignora linhas que não têm o número esperado de colunas

            # Extrai as colunas necessárias
            opcao = cells[1].inner_text().strip()
            usuario = cells[3].inner_text().strip()
            data_hora_solicitacao = cells[2].inner_text().strip()
            situacao = cells[6].inner_text().strip()

            # Verifica se a linha atende aos critérios de "opcao" e "usuario"
            if opcao == criterios_opcao and usuario == criterios_usuario:
                todas_linhas.append((index, row, data_hora_solicitacao, situacao))  # Inclui o índice, a linha, a data e a situação

        # Ordena todas as linhas filtradas pela coluna de Data/Hora Solicitação (do mais recente para o mais antigo)
        todas_linhas.sort(key=lambda x: x[2], reverse=True)

        # Filtragem condicional baseada em config_verificar_todos
        if config_verificar_todos:
            # Filtrar apenas as linhas do ano atual e do ano anterior
            ano_atual = datetime.today().year
            ano_anterior = ano_atual - 1
            filtered_rows_atualizado = [
                row for row in todas_linhas 
                if int(row[2].split('/')[-1]) in {ano_atual, ano_anterior}
            ]
        else:
            # Seleciona apenas as 8 linhas mais recentes
            filtered_rows_atualizado = todas_linhas[:9]

        # Verifica se todas as linhas selecionadas têm situação "Concluído"
        todas_concluidas = all(situacao == "Concluído" for _, _, _, situacao in filtered_rows_atualizado)

        # Se todas as linhas estiverem concluídas, saímos do loop
        if todas_concluidas:
            print("Todas as linhas selecionadas estão com situação 'Concluído'.")
            break
        else:
            print("Aguardando todas as linhas serem concluídas...")
            new_page.click(update_button_selector)  # Clica no botão para atualizar a tabela
            time.sleep(20)  # Aguarda a atualização da tabela antes de verificar novamente

    return filtered_rows_atualizado  # Retorna as linhas selecionadas com situação "Concluído"
