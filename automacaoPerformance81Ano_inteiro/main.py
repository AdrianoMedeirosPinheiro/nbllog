from playwright.sync_api import sync_playwright
import time
import os
import shutil
from datetime import datetime

import limpar_diretorio
import tratamentossw
import consolidar_banco

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

def executar_script():
    with sync_playwright() as p:
        # Inicialização do navegador
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        # Acesso ao sistema
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
        
        # Acessa o link de relatório
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
        
        # Limpa diretório de download
        download_directory = "/nbllog/automacaoPerformance81Ano_inteiro/base" 
        limpar_diretorio.limpar_diretorio(download_directory)
        time.sleep(2)

        # Seleciona todas as linhas da tabela (exceto cabeçalhos, se houver).
        linhas = new2_page.query_selector_all("#tblsr > tbody > tr")
        
        for linha in linhas:
            # Ajuste o td:nth-child(X) de acordo com a posição da coluna 'Data e hora'
            data_hora_cell = linha.query_selector("td:nth-child(5)")
            if not data_hora_cell:
                continue
            
            data_hora_texto = data_hora_cell.inner_text().strip()
            # Exemplo de data_hora_texto = "10/01/25 01:33"
            
            # Tenta converter a string para datetime, assumindo formato "dd/mm/yy HH:MM"
            try:
                dt = datetime.strptime(data_hora_texto, "%d/%m/%y %H:%M")
            except ValueError:
                continue
            
            # Verifica se o ano convertido é 2025
            if dt.year == 2025:
                # Se for 2025, clica para fazer o download
                botao_download = linha.query_selector("td:nth-child(2) > div")
                if botao_download:
                    with new2_page.expect_download() as download_info:
                        botao_download.click()
                        print(f"Download iniciado para {data_hora_texto}")
                    
                    time.sleep(4)
                    download_file = download_info.value
                    original_path = download_file.path()
                    
                    if original_path:
                        # Separa o nome do arquivo e a extensão atual
                        filename, _ = os.path.splitext(download_file.suggested_filename)
                        
                        # Define o novo nome do arquivo com a extensão .sswweb
                        new_filename = f"{filename}.sswweb"
                        
                        # Caminho de destino no diretório de download especificado
                        destination_path = os.path.join(download_directory, new_filename)
                        
                        # Move o arquivo baixado para o diretório com o novo nome
                        shutil.move(original_path, destination_path)
                        print(f"Arquivo movido e renomeado para: {destination_path}")
                    else:
                        print("Falha ao obter o caminho do arquivo baixado.")

        # Após baixar todos os relatórios, processar arquivos
        time.sleep(2)
        diretorio = r'/nbllog/automacaoPerformance81Ano_inteiro/base'
        df_final = tratamentossw.processar_arquivos_ssw(diretorio)
        caminho_saida = os.path.join(diretorio, 'resultado_processado.xlsx')
        df_final.to_excel(caminho_saida, index=False)
        
        time.sleep(2)
        
        # Fecha o navegador
        browser.close()
        
        ## CONEXÃO COM BANCO AZURE ##
        # Defina os parâmetros do banco PostgreSQL
        diretorio = '/nbllog/automacaoPerformance81Ano_inteiro/base/resultado_processado.xlsx'  
        tabela_destino = 'relatoriosperformance81'
        
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
            query={"sslmode": "disable"}
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


if __name__ == "__main__":
    executar_script()
