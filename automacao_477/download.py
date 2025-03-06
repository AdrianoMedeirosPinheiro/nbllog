import os
import time
import shutil  # Para mover arquivos
from datetime import datetime

def iniciar_download(new_page, filtered_rows, download_directory, config_baixar_todos=False, loading_selector="#procimg"):
    """
    Função para iniciar o download de arquivos após confirmar que todas as linhas estão concluídas.
    
    Parâmetros:
    - new_page: Página atual do Playwright onde os downloads serão iniciados.
    - filtered_rows: Lista das linhas (com índices) que atendem aos critérios para download.
    - download_directory: Diretório onde os downloads devem ser armazenados.
    - config_baixar_todos: Se True, baixa arquivos de todas as linhas do ano atual e do ano anterior; se False, baixa apenas das 3 últimas linhas.
    - loading_selector: Seletor do elemento de carregamento que deve ser verificado antes do clique no botão de download.
    """

    # Cria o diretório de download, caso não exista
    if not os.path.exists(download_directory):
        os.makedirs(download_directory)

    downloaded_files = 0  # Contador para os downloads realizados

    # Filtragem condicional das linhas para download
    if config_baixar_todos:
        # Filtrar apenas as linhas do ano atual e do ano anterior
        ano_atual = datetime.today().year
        ano_anterior = ano_atual - 1
        filtered_rows = [
            row for row in filtered_rows
            if int(row[2].split('/')[-1]) in {ano_atual, ano_anterior}
        ]
    else:
        # Filtrar apenas as 9 últimas linhas
        filtered_rows = filtered_rows[:9]

    for line_index, row, data_hora_solicitacao in filtered_rows:
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
                        # Separa o nome do arquivo e a extensão atual
                        filename, _ = os.path.splitext(download.suggested_filename)
                        
                        # Define o novo nome do arquivo com a extensão .sswweb
                        new_filename = f"{filename}.sswweb"
                        
                        # Caminho de destino no diretório de download especificado
                        destination_path = os.path.join(download_directory, new_filename)
                        
                        # Move o arquivo baixado para o diretório com o novo nome
                        shutil.move(original_path, destination_path)
                        print(f"Arquivo movido e renomeado para: {destination_path}")
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

    # Verifica se foram realizados todos os downloads esperados
    expected_downloads = 13 if config_baixar_todos else 3
    downloaded_files_count = len([f for f in os.listdir(download_directory) if os.path.isfile(os.path.join(download_directory, f))])
    if downloaded_files_count == expected_downloads:
        print(f"Todos os {expected_downloads} arquivos foram baixados com sucesso no diretório: {download_directory}")
    else:
        print(f"Atenção: apenas {downloaded_files_count} de {expected_downloads} arquivos foram baixados.")

# Exemplo de uso:
# Para baixar apenas as 3 últimas linhas
# iniciar_download(new_page, filtered_rows, download_directory, config_baixar_todos=False)

# Para baixar as linhas do ano atual e do ano anterior
# iniciar_download(new_page, filtered_rows, download_directory, config_baixar_todos=True)
