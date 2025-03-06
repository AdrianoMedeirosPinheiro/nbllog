import os
import shutil

def limpar_diretorio(diretorio):
    """
    Remove todos os arquivos e pastas no diretório especificado.
    """
    # Verifica se o diretório existe
    if not os.path.exists(diretorio):
        print(f"O diretório {diretorio} não existe.")
        return
    
    for arquivo in os.listdir(diretorio):
        caminho_arquivo = os.path.join(diretorio, arquivo)
        try:
            # Remove arquivos individuais
            if os.path.isfile(caminho_arquivo) or os.path.islink(caminho_arquivo):
                os.remove(caminho_arquivo)
            # Remove diretórios
            elif os.path.isdir(caminho_arquivo):
                shutil.rmtree(caminho_arquivo)
        except Exception as e:
            print(f"Não foi possível remover {caminho_arquivo}: {e}")

# Configura o diretório de download
#download_directory = "C:\\automacao\\base"

# Limpa o diretório antes de iniciar o download
#limpar_diretorio(download_directory)
