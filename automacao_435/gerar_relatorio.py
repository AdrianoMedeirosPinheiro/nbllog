from datetime import datetime, timedelta
import time
import os
import shutil  # Para mover arquivos

def gerar_relatorios(popup_page, download_directory):
    """
    Função para gerar relatórios e armazenar o download em um diretório especificado.

    Parâmetros:
    - popup_page: Página atual do Playwright onde o download será iniciado.
    - download_directory: Diretório onde o download deve ser armazenado.
    """

    # Cria o diretório de download, caso não exista
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    # Limpa e preenche os campos adicionais
    popup_page.fill('#t_tp_fil', '')
    popup_page.fill('#t_tp_fil', 'R')

    popup_page.fill('#t_tp_cliente', '')
    popup_page.fill('#t_tp_cliente', 'C')

    popup_page.fill('#t_tp_cli_fat', '')
    popup_page.fill('#t_tp_cli_fat', 'T')

    popup_page.fill('#t_situacao_ctrc', '')
    popup_page.fill('#t_situacao_ctrc', 'I')

    popup_page.fill('#t_periodicidade', '')
    popup_page.fill('#t_periodicidade', 'T')

    popup_page.fill('#t_rel_lista', '')
    popup_page.fill('#t_rel_lista', 'T')

    popup_page.fill('#t_cons_bloqueados', '')
    popup_page.fill('#t_cons_bloqueados', 'N')

    popup_page.fill('#t_cons_a_vista', '')
    popup_page.fill('#t_cons_a_vista', 'S')

    popup_page.fill('#t_tp_classificacao', '')
    popup_page.fill('#t_tp_classificacao', 'F')

    popup_page.fill('#t_excel', '')
    popup_page.fill('#t_excel', 'S')

    popup_page.fill('#t_ler_morto', '')
    popup_page.fill('#t_ler_morto', 'N')

    # Clica para gerar o relatório e captura o download
    with popup_page.expect_download() as download_info:
        popup_page.click('#btn_env')
        print("Download iniciado.")

    time.sleep(20)

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
