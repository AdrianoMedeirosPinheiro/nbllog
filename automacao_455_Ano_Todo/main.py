from playwright.sync_api import sync_playwright
import time
import download
import verificar_concluida
import ultimas_linhas
import carregar_tabela
import gerar_relatorio
import limpar_diretorio
from consolidar_banco import consolidar_arquivos_sswweb_para_postgres
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://sistema.ssw.inf.br/bin/ssw0422")
    print(page.title())
    time.sleep(2)
    page.wait_for_selector('#\\31')
    page.fill('#\\31', 'TDT')
    page.fill('#\\32', '02898809209')
    page.fill('#\\33', 'medeiros')
    page.fill('#\\34', '123456')
    page.click('#\\35')
    time.sleep(2)
    page.wait_for_selector('#\\33')
    page.fill('#\\33', '455')
    page.press('#\\33', 'Enter')
    with page.expect_popup() as popup_info:
        page.press('#\\33', 'Enter')

    popup_page = popup_info.value
    time.sleep(2)
    popup_page.wait_for_selector('#\\39')

    gerar_relatorio.gerar_relatorios_ano_especifico(popup_page, 2022)

    with popup_page.expect_popup() as popup_info:
        popup_page.click('#\\34 2')
    new_page = popup_info.value
    time.sleep(5)
    new_page.click('#\\32')

    rows, criterios_usuario, criterios_opcao, criterios_situacao = carregar_tabela.carregar_e_filtrar_tabela(new_page)
    filtered_rows = ultimas_linhas.filtrar_ultimas_linhas(rows, criterios_opcao, criterios_usuario)
    filtered_rows_atualizado = verificar_concluida.verificar_situacao_concluida(new_page, criterios_opcao, criterios_usuario)
    download_directory = "/nbllog/automacao_455_Ano_Todo/base" 
    limpar_diretorio.limpar_diretorio(download_directory)
    time.sleep(2)
    download.iniciar_download(new_page, filtered_rows_atualizado, download_directory)
    time.sleep(2)
    #input("Pressione Enter para fechar o navegador...")  # Pausa antes de fechar o navegador
    browser.close()
    time.sleep(2)

    ## CONEXÃO COM BANCO AZURE ##

    # Define o ano específico para exclusão
    ano_especifico = 2022

    # Defina os parâmetros do banco PostgreSQL
    diretorio = '/nbllog/automacao_455_Ano_Todo/base'  # Caminho para o diretório dos arquivos .sswweb
    tabela_destino = 'relatorios455'  # Nome da tabela no PostgreSQL

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
    consolidar_arquivos_sswweb_para_postgres(diretorio, tabela_destino, engine, ano_especifico)

                    