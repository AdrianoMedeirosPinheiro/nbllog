from playwright.sync_api import sync_playwright
import time
import gerar_relatorio
import limpar_diretorio
from consolidar_banco import consolidar_arquivos_sswweb_para_postgres
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto("https://sistema.ssw.inf.br/bin/ssw0422")
    print(page.title())
    time.sleep(10)
    page.wait_for_selector('#\\31')
    page.fill('#\\31', 'TDT')
    page.fill('#\\32', '02898809209')
    page.fill('#\\33', 'medeiros')
    page.fill('#\\34', '123456')
    page.click('#\\35')
    time.sleep(10)
    page.wait_for_selector('#\\33')
    page.fill('#\\33', '435')
    page.press('#\\33', 'Enter')
    time.sleep(10)
    with page.expect_popup() as popup_info:
        time.sleep(10)
        page.press('#\\33', 'Enter')
    time.sleep(10)
    popup_page = popup_info.value
    time.sleep(10)
    popup_page.wait_for_selector('#t_banco')

    download_directory = "/nbllog/automacao_435/base"
    limpar_diretorio.limpar_diretorio(download_directory)
    time.sleep(10)

    gerar_relatorio.gerar_relatorios(popup_page, download_directory)

    browser.close()
    time.sleep(10)

    ## CONEXÃO COM BANCO AZURE ##

    # Defina os parâmetros do banco PostgreSQL
    diretorio = '/nbllog/automacao_435/base'  # Caminho para o diretório dos arquivos .sswweb
    tabela_destino = 'relatorios435'  # Nome da tabela no PostgreSQL

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
    consolidar_arquivos_sswweb_para_postgres(diretorio, tabela_destino, engine)

                    