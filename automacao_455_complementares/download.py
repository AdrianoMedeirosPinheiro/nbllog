import os
import time
import shutil  # Para mover arquivos
from playwright.sync_api import sync_playwright

def iniciar_download(new_page, filtered_rows, download_directory, loading_selector="#procimg"):
    """
    Função para iniciar o download de arquivos após confirmar que todas as linhas estão concluídas.
    
    Parâmetros:
    - new_page: Página atual do Playwright onde os downloads serão iniciados.
    - filtered_rows: Lista das linhas (com índices) que atendem aos critérios para download.
    - download_directory: Diretório onde os downloads devem ser armazenados.
    - loading_selector: Seletor do elemento de carregamento que deve ser verificado antes do clique no botão de download.
    """

    # Cria o diretório de download, caso não exista
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    downloaded_files = 0  # Contador para os downloads realizados

    for line_index, row, _, _ in filtered_rows:
        try:
            download_button_selector = f'#tblsr > tbody > tr:nth-child({line_index}) > td:nth-child(9) > div > a > u'
            
            max_attempts = 5
            for attempt in range(max_attempts):
                try:
                    # Aguarda intermitentemente até que o carregamento desapareça
                    for check in range(20):  # Tentativas para ver se o carregamento desapareceu
                        if not new_page.is_visible(loading_selector):
                            break  # Sai do loop se o carregamento desapareceu
                        else:
                            time.sleep(10)  # Espera 10 segundos e verifica novamente
                    
                    # Tenta clicar no botão de download
                    with new_page.expect_download() as download_info:
                        download_button = new_page.wait_for_selector(download_button_selector, state="visible", timeout=5000)
                        download_button.click()
                        print(f"Download iniciado na linha {line_index}")

                    download = download_info.value
                    original_path = download.path()
                    if original_path:  # Se o arquivo foi baixado com sucesso
                        # Caminho de destino no diretório de download especificado
                        destination_path = os.path.join(download_directory, download.suggested_filename)
                        shutil.move(original_path, destination_path)  # Move o arquivo baixado para o diretório
                        print(f"Arquivo movido para: {destination_path}")
                        downloaded_files += 1
                        time.sleep(20)
                    
                    time.sleep(20)  # Pausa entre os downloads para evitar sobrecarga
                    break  # Sai do loop de tentativas se o clique foi bem-sucedido

                except Exception as e:
                    print(f"Tentativa {attempt + 1} falhou na linha {line_index}: {e}")
                    
                    if attempt < max_attempts - 1:
                        time.sleep(5)  # Pausa antes de tentar novamente
                    else:
                        print(f"Falha ao clicar no botão de download após {max_attempts} tentativas na linha {line_index}")
                        
        except Exception as e:
            print(f"Erro inesperado ao processar a linha {line_index}: {e}")

    # Verifica se foram realizados 10 downloads
    downloaded_files_count = len([f for f in os.listdir(download_directory) if os.path.isfile(os.path.join(download_directory, f))])
    if downloaded_files_count == 13:
        print(f"Todos os 13 arquivos foram baixados com sucesso no diretório: {download_directory}")
    else:
        print(f"Atenção: apenas {downloaded_files_count} de 13 arquivos foram baixados.")

# Exemplo de uso:
# download_directory = "C:/meu_diretorio_de_downloads"
# iniciar_download(new_page, filtered_rows_atualizado, download_directory)
