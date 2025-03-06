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
    new_page.fill('#\\31', '81')
    time.sleep(2)
    with new_page.expect_popup() as popup_info:
        new_page.click('#\\32')
    new2_page = popup_info.value
    
    time.sleep(2)
    download_directory = "/nbllog/automacaoPerformance81/base" 
    limpar_diretorio.limpar_diretorio(download_directory)
    time.sleep(2)

        # Clica para gerar o relatório e captura o download
    with new2_page.expect_download() as download_info:
        new2_page.click('#tblsr > tbody > tr:nth-child(2) > td:nth-child(2) > div')
        print("Download iniciado.")

    time.sleep(10)
    download_directory = '/nbllog/automacaoPerformance81/base'
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

    time.sleep(2)
    diretorio = r'/nbllog/automacaoPerformance81/base'
    df_final = tratamentossw.processar_arquivo_ssw(diretorio)
    caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
    df_final.to_excel(caminho_saida, index=False)

    # input("Pressione Enter para fechar o navegador...")  # Pausa antes de fechar o navegador

    time.sleep(2)

    browser.close()

    ## CONEXÃO COM BANCO AZURE ##

    # Defina os parâmetros do banco PostgreSQL
    diretorio = '/nbllog/automacaoPerformance81/base/resultado_processado.xlsx'  # Caminho para o diretório dos arquivos .sswweb
    tabela_destino = 'relatoriosperformance81'  # Nome da tabela no PostgreSQL

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

                    