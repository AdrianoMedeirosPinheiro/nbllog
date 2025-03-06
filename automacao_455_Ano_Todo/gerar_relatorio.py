import time
from datetime import datetime
import calendar

def dias_no_mes(ano, mes):
    """Retorna o número de dias em um mês específico de um ano."""
    _, num_dias = calendar.monthrange(ano, mes)
    return num_dias

def gerar_relatorios_ano_especifico(popup_page, ano_especifico):
    """
    Gera relatórios para todos os meses de um ano específico.
    
    :param popup_page: Objeto que representa a página para interação.
    :param ano_especifico: Ano para o qual os relatórios serão gerados.
    """
    for mes in range(1, 13):
        primeiro_dia = 1
        ultimo_dia = dias_no_mes(ano_especifico, mes)

        # Formata as datas no formato ddmmaa
        data_inicio_str = f"{primeiro_dia:02d}{mes:02d}{ano_especifico % 100:02d}"
        data_fim_str = f"{ultimo_dia:02d}{mes:02d}{ano_especifico % 100:02d}"

        # Preenche os campos de data de início e fim
        popup_page.fill('#\\39', data_inicio_str)
        popup_page.fill('#\\31 0', data_fim_str)

        # Limpa e preenche os campos adicionais
        popup_page.fill('#\\31 1', '')
        popup_page.fill('#\\31 2', '')
        popup_page.fill('#\\32 1', 'T')
        popup_page.fill('#\\33 5', 'E')

        time.sleep(2)
        popup_page.fill('#\\33 7', 'A')
        time.sleep(2)

        # Usa o locator para pressionar a tecla Tab
        locator = popup_page.locator('#\\33 7')
        locator.press('Tab')
        time.sleep(2)

        # Preenche os campos finais
        popup_page.fill('#\\33 8', 'B')
        time.sleep(2)
        popup_page.fill('#\\33 9', 'F')

        # Clica para gerar o relatório
        popup_page.click('#\\34 0')

        time.sleep(2)

        # Clica para confirmar ou fechar alguma janela, se necessário
        popup_page.click('#\\30')

        # Log do progresso
        print(f"Relatório para o mês {mes}/{ano_especifico} gerado até {data_fim_str}. Aguardando 1 minuto antes de prosseguir...")
        
        # Aguardar 1 minuto antes de prosseguir para o próximo relatório
        time.sleep(60)

# Exemplo de uso:
# gerar_relatorios_ano_especifico(popup_page, 2024)