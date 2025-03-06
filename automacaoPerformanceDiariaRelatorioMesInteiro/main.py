from datetime import datetime
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

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://sistema.ssw.inf.br/bin/ssw0422")
    time.sleep(3)
    # Preenche e faz login
    page.fill('#\\31', 'TDT')
    page.fill('#\\32', '02898809209')
    page.fill('#\\33', 'medeiros')
    page.fill('#\\34', '123456')
    page.click('#\\35')
    page.wait_for_selector('#frm > div:nth-child(16) > p:nth-child(29) > a:nth-child(1)')
    time.sleep(3)
    # Abre popup 1
    with page.expect_popup() as popup_info:
        page.click('#frm > div:nth-child(16) > p:nth-child(29) > a:nth-child(1)')
    new_page = popup_info.value
    time.sleep(3)
    new_page.wait_for_selector('#\\35')
    new_page.click('#\\35')
    time.sleep(3)
    # Preenche campo para relatório "12"
    new_page.fill('#\\31', '12')
    time.sleep(3)
    # Abre popup 2
    with new_page.expect_popup() as popup_info:
        new_page.click('#\\32')
    new2_page = popup_info.value
    time.sleep(3)
    # Limpa diretório de download
    download_directory = r"/nbllog/automacaoPerformanceDiariaRelatorioMesInteiro/base" 
    limpar_diretorio.limpar_diretorio(download_directory)
    time.sleep(3)
    # Seleciona todas as linhas do corpo da tabela (#tblsr > tbody > tr)
    rows = new2_page.query_selector_all("#tblsr > tbody > tr")
    time.sleep(3)
    # Para cada linha (exceto cabeçalho), capturar data na 5ª coluna e comparar com mês/ano atual
    for i, row in enumerate(rows, start=1):
        if i == 1:
            # Se a primeira linha for cabeçalho, pular
            continue
        
        # Monta seletor para a 5ª coluna
        date_selector = f"#tblsr > tbody > tr:nth-child({i}) > td:nth-child(5)"
        date_element = new2_page.query_selector(date_selector)
        if not date_element:
            continue
        
        date_text = date_element.inner_text().strip()  # Ex: "07/02/25 12:54"
        
        # Tenta converter a string em datetime
        # Ajuste o formato para corresponder ao padrão exibido (dia/mês/ano hora:minuto)
        try:
            date_dt = datetime.strptime(date_text, "%d/%m/%y %H:%M")
        except ValueError:
            # Se não conseguir converter, pula
            continue
        
        # Verifica se é do mês e ano atuais
        now = datetime.now()
        if date_dt.month == now.month and (date_dt.year % 100) == (now.year % 100):
            # Inicia download (2ª coluna tem o "div" que dispara o download)
            # Ajuste se a estrutura do HTML mudar
            download_selector = f"#tblsr > tbody > tr:nth-child({i}) > td:nth-child(2) > div"
            
            with new2_page.expect_download() as download_info:
                new2_page.click(download_selector)
                print(f"[INFO] Download iniciado para linha {i} (Data: {date_text}).")
            
            # Espera terminar o download
            file_download = download_info.value
            file_path = file_download.path()
            if file_path:
                # Separa o nome do arquivo e a extensão atual
                filename, _ = os.path.splitext(file_download.suggested_filename)
                
                # Define o novo nome do arquivo com a extensão .sswweb
                new_filename = f"{filename}.sswweb"
                
                # Caminho de destino no diretório de download especificado
                destino = os.path.join(download_directory, new_filename)
                
                # Move o arquivo baixado para o diretório com o novo nome
                shutil.move(file_path, destino)
                print(f"[INFO] Arquivo movido e renomeado para: {destino}")
            else:
                print("[ERRO] Falha ao obter o caminho do arquivo baixado.")
            
            # Pausa leve para evitar congestionamento de downloads
            time.sleep(3)



    time.sleep(2)
    diretorio = r'/nbllog/automacaoPerformanceDiariaRelatorioMesInteiro/base'
    df_final = tratamentossw.processar_varios_arquivos_ssw(diretorio)
    caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
    df_final.to_excel(caminho_saida, index=False)


    time.sleep(2)

    browser.close()

    #input("Pressione Enter para fechar o navegador...")  # Pausa antes de fechar o navegador

    ## CONEXÃO COM BANCO AZURE ##

    # Defina os parâmetros do banco PostgreSQL
    diretorio = '/nbllog/automacaoPerformanceDiariaRelatorioMesInteiro/base/resultado_processado.xlsx'  # Caminho para o diretório dos arquivos .sswweb
    tabela_destino = 'relatoriosperformancediaria'  # Nome da tabela no PostgreSQL

    # Parâmetros de conexão
    usuario = "nblssw"
    senha = "Marituba2030@"  # Substitua pela sua senha
    host = "nblssw.postgres.database.azure.com"    # Exemplo: "nbl.postgres.database.azure.com"
    porta = 5432
    banco = "nbl"

    # Utilize URL.create para construir a string de conexão
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
        # Cria o engine usando o URL criado
        engine = create_engine(url_object)

        # Testa a conexão com o SQLAlchemy
        with engine.connect() as conn:
            print("Conexão com o PostgreSQL via SQLAlchemy estabelecida com sucesso!")
    except Exception as e:
        print("Erro ao conectar via SQLAlchemy:", e)

    # Chama a função para processar e inserir os arquivos na tabela PostgreSQL
    consolidar_banco.consolidar_resultado_processado(diretorio, tabela_destino, engine)

                    