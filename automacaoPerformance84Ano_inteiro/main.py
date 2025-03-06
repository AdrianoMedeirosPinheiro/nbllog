from playwright.sync_api import sync_playwright
import time
import download
import limpar_diretorio
import tratamentossw
import consolidar_banco
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os
import shutil  # Para mover arquivos
from datetime import datetime

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://sistema.ssw.inf.br/bin/ssw0422")
    print(page.title())
    time.sleep(4)
    page.wait_for_selector('#\\31')
    page.fill('#\\31', 'TDT')
    page.fill('#\\32', '02898809209')
    page.fill('#\\33', 'medeiros')
    page.fill('#\\34', '123456')
    page.click('#\\35')
    time.sleep(4)
    
    page.wait_for_selector('#frm > div:nth-child(16) > p:nth-child(29) > a:nth-child(1)')
    time.sleep(3)
    with page.expect_popup() as popup_info:
        page.click('#frm > div:nth-child(16) > p:nth-child(29) > a:nth-child(1)')
    new_page = popup_info.value
    
    time.sleep(3)
    new_page.wait_for_selector('#\\35')
    new_page.click('#\\35')
    time.sleep(3)
    new_page.fill('#\\31', '84')
    time.sleep(2)
    
    with new_page.expect_popup() as popup_info:
        new_page.click('#\\32')
    new2_page = popup_info.value
    
    time.sleep(2)
    download_directory = "/nbllog/automacaoPerformance84Ano_inteiro/base" 
    limpar_diretorio.limpar_diretorio(download_directory)
    time.sleep(2)
    
    # -------------------------------------------
    # CAPTURA TODAS AS LINHAS E BAIXA SOMENTE AS DE 2025
    # -------------------------------------------
    
    # Seleciona todas as linhas da tabela (exceto cabeçalhos, se houver).
    linhas = new2_page.query_selector_all("#tblsr > tbody > tr")
    
    for linha in linhas:
        # Ajuste td:nth-child(5) se necessário, conforme a posição da coluna "Data e hora"
        data_hora_cell = linha.query_selector("td:nth-child(5)")
        if not data_hora_cell:
            continue
        
        data_hora_texto = data_hora_cell.inner_text().strip()
        # Exemplo de data: "10/01/25 01:33"
        
        # Converte para datetime (ajuste o formato se precisar incluir segundos, etc.)
        try:
            dt = datetime.strptime(data_hora_texto, "%d/%m/%y %H:%M")
        except ValueError:
            # Se não conseguir converter, passa para a próxima linha
            continue
        
        # Verifica se o ano é 2025
        if dt.year == 2025:
            # Localiza o "botão" ou elemento de download (td:nth-child(2) > div, por exemplo).
            botao_download = linha.query_selector("td:nth-child(2) > div")
            if botao_download:
                with new2_page.expect_download() as download_info:
                    botao_download.click()
                    print(f"Download iniciado para {data_hora_texto}")
                
                time.sleep(5)  # Ajuste se precisar de mais/menos tempo
                
                # Move o arquivo baixado para a pasta de destino
                download = download_info.value
                original_path = download.path()
                if original_path:
                    # Separa o nome do arquivo e a extensão atual
                    filename, _ = os.path.splitext(download.suggested_filename)
                    
                    # Define o novo nome do arquivo com a extensão .sswweb
                    new_filename = f"{filename}.sswweb"
                    
                    # Caminho de destino no diretório de download especificado
                    destination_path = os.path.join(download_directory, new_filename)
                    
                    # Move o arquivo baixado para o diretório com o novo nome
                    shutil.move(original_path, destination_path)
                    print(f"Arquivo movido e renomeado para: {destination_path}")
                else:
                    print("Falha ao obter o caminho do arquivo baixado.")

    # Prossegue com o tratamento normal...
    time.sleep(2)
    diretorio = r'/nbllog/automacaoPerformance84Ano_inteiro/base'
    
    # Processa o(s) arquivo(s) baixado(s) para Excel
    df_final = tratamentossw.processar_arquivos_ssw(diretorio)
    caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
    df_final.to_excel(caminho_saida, index=False)
    
    time.sleep(2)
    browser.close()
    
    # ------------------------------------------------------
    # CONEXÃO COM BANCO AZURE
    # ------------------------------------------------------
    diretorio = '/nbllog/automacaoPerformance84Ano_inteiro/base/resultado_processado.xlsx'
    tabela_destino = 'relatoriosperformance84'
    
    # Parâmetros de conexão
    usuario = "nblssw"
    senha = "Marituba2030@"  # Substitua pela sua senha
    host = "nblssw.postgres.database.azure.com"    # Exemplo: "nbl.postgres.database.azure.com"
    porta = 5432
    banco = "nbl"
    
    # Monta a string de conexão com o URL.create
    url_object = URL.create(
        drivername="postgresql+psycopg2",
        username=usuario,
        password=senha,
        host=host,
        port=porta,
        database=banco,
        query={"sslmode": "require"}
    )
    
    try:
        engine = create_engine(url_object)
        with engine.connect() as conn:
            print("Conexão com o PostgreSQL via SQLAlchemy estabelecida com sucesso!")
    except Exception as e:
        print("Erro ao conectar via SQLAlchemy:", e)
    
    # Insere no banco
    consolidar_banco.consolidar_resultado_processado(diretorio, tabela_destino, engine)
