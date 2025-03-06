@echo off
setlocal

REM Vari�vel de controle para indicar se o script foi executado com sucesso
set executed=0
echo [%date% %time%] Iniciando o loop de verifica��o de hor�rios...

:loop
REM Obtemos a hora atual no formato HH
for /f "tokens=1-2 delims=:" %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
)

REM Removemos zeros � esquerda para comparar horas corretamente
set /a hour=%hour%
set /a minute=%minute%

REM Log de debug para verificar hor�rio atual
echo [%date% %time%] Hora atual: %hour%:%minute% - Executado anteriormente: %executed%

REM Verifica se a hora atual � igual a qualquer um dos hor�rios desejados e se ainda n�o foi executado
if %hour%==6 if %minute%==0 if %executed%==0 goto run_script
if %hour%==12 if %minute%==0 if %executed%==0 goto run_script
if %hour%==22 if %minute%==0 if %executed%==0 goto run_script

REM Redefine a vari�vel executed quando o hor�rio atual for diferente dos hor�rios de execu��o
if not %hour%==6 if not %hour%==9 if not %hour%==12 if not %hour%==15 if not %hour%==20 (
    if %executed%==1 (
        echo [%date% %time%] Redefinindo estado de execu��o.
        set executed=0
    )
)

REM Aguarda 60 segundos antes de verificar novamente
timeout /t 60 /nobreak
goto loop

:run_script
echo [%date% %time%] Executando o script...

REM Caminho para o arquivo conda.bat, que � necess�rio para ativar ambientes no CMD
echo [%date% %time%] Ativando o ambiente Conda...
CALL C:\ProgramData\Miniconda3\condabin\conda.bat activate Extracao
IF ERRORLEVEL 1 (
    echo [%date% %time%] Erro: N�o foi poss�vel ativar o ambiente Conda.
    pause
    goto end_script
)

REM Muda para o diret�rio onde o script Python est� localizado
echo [%date% %time%] Mudando para o diret�rio do script Python...
cd /d C:\Users\nblbi\Desktop\bot\automacao_455
IF ERRORLEVEL 1 (
    echo [%date% %time%] Erro: N�o foi poss�vel mudar para o diret�rio especificado.
    pause
    goto end_script
)

REM Executa o script Python
echo [%date% %time%] Executando o script Python main.py...
python main.py
IF ERRORLEVEL 1 (
    echo [%date% %time%] Erro: O script Python falhou ao ser executado.
    pause
    goto end_script
)

REM Mensagem de sucesso
echo [%date% %time%] Script executado com sucesso!

:end_script
REM Marca como executado para n�o repetir at� o pr�ximo hor�rio
set executed=1
echo [%date% %time%] Marcando estado de execu��o como conclu�do.

REM Aguarda 60 segundos antes de voltar ao loop
timeout /t 60 /nobreak
goto loop
