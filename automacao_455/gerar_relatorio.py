import time
from datetime import datetime, timedelta
import calendar

def dias_no_mes(ano, mes):
    _, num_dias = calendar.monthrange(ano, mes)
    return num_dias

def gerar_relatorios_ultimos_12_meses(popup_page):
    hoje = datetime.today()
    data_inicio = hoje.replace(day=1) - timedelta(days=365)
    data_fim = hoje

    mes_ano_inicio = data_inicio.replace(day=1)
    mes_ano_fim = data_fim.replace(day=1)

    delta = (mes_ano_fim.year - mes_ano_inicio.year) * 12 + (mes_ano_fim.month - mes_ano_inicio.month)

    for i in range(delta + 1):
        mes_ano_atual = mes_ano_inicio + timedelta(days=31*i)
        ano = mes_ano_atual.year
        mes = mes_ano_atual.month

        primeiro_dia = 1
        ultimo_dia = dias_no_mes(ano, mes)

        if ano == data_fim.year and mes == data_fim.month:
            ultimo_dia = data_fim.day

        data_inicio_str = f"{primeiro_dia:02d}{mes:02d}{ano % 100:02d}"
        data_fim_str = f"{ultimo_dia:02d}{mes:02d}{ano % 100:02d}"

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
        
        # Aguardar 1 minuto antes de prosseguir para o próximo relatório
        print(f"Relatório para o mês {mes}/{ano} gerado até {data_fim_str}. Aguardando 1 minuto antes de prosseguir...")
        time.sleep(60)
