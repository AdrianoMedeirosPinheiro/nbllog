from datetime import datetime, timedelta
import time

def dias_no_mes(ano, mes):
    """Retorna o último dia de um mês específico."""
    if mes == 12:
        return 31
    return (datetime(ano, mes + 1, 1) - timedelta(days=1)).day

def gerar_relatorios(popup_page, config_baixar_todos):
    hoje = datetime.today()
    ano_atual = hoje.year
    mes_atual = hoje.month

    # Verificar se todos os meses do ano atual e do ano anterior devem ser baixados
    if config_baixar_todos:
        data_inicio = datetime(ano_atual - 1, 1, 1)  # Janeiro do ano anterior
    else:
        # Somente os três últimos meses (mês atual e dois anteriores)
        data_inicio = (hoje.replace(day=1) - timedelta(days=240))  # Dois meses antes

    data_fim = hoje
    delta = (data_fim.year - data_inicio.year) * 12 + (data_fim.month - data_inicio.month)

    for i in range(delta + 1):
        mes_ano_atual = data_inicio + timedelta(days=31 * i)
        ano = mes_ano_atual.year
        mes = mes_ano_atual.month

        if ano == data_fim.year and mes == data_fim.month:
            ultimo_dia = data_fim.day

        data_inicio_str = f"{mes:02d}{ano % 100:02d}"

        # Preenche os campos de data de início e fim
        popup_page.fill('#mes_comp', data_inicio_str)

        # Limpa e preenche os campos adicionais
        popup_page.fill('#sit_desp', '')
        popup_page.fill('#sit_desp', 'T')

        # Clica para gerar o relatório
        popup_page.click('#link_excel')

        time.sleep(2)

        # Clica para confirmar ou fechar alguma janela, se necessário
        popup_page.click('#\\30')
        
        # Aguardar 1 minuto antes de prosseguir para o próximo relatório
        print(f"Relatório para o mês {mes}/{ano} gerado até {data_inicio_str}. Aguardando 1 minuto antes de prosseguir...")
        time.sleep(40)

