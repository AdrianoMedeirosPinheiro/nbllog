@echo off
setlocal

REM Variável de controle para indicar se o script foi executado com sucesso
set executed=0
echo [%date% %time%] Iniciando o loop de verificação de horários...

:loop
REM Obtém a hora atual no formato HH
for /f "tokens=1-2 delims=:" %%a in ("%time%") do (
    set hour=%%a
    set minute=%%b
)

REM Remove zeros à esquerda para comparar horas corretamente
set /a hour=%hour%
set /a minute=%minute%

REM Log de debug para verificar horário atual
echo [%date% %time%] Hora atual: %hour%:%minute% - Executado anteriormente: %executed%

REM Verifica se a hora atual é igual a qualquer um dos horários desejados e se ainda não foi executado
if %hour%==7 if %minute%==0 if %executed%==0 goto run_script
if %hour%==9 if %minute%==0 if %executed%==0 goto run_script
if %hour%==11 if %minute%==0 if %executed%==0 goto run_script
if %hour%==13 if %minute%==0 if %executed%==0 goto run_script
if %hour%==15 if %minute%==0 if %executed%==0 goto run_script

REM Redefine a variável executed quando o horário atual for diferente dos horários de execução
if not %hour%==7 if not %hour%==9 if not %hour%==11 if not %hour%==13 if not %hour%==15 (
    if %executed%==1 (
        echo [%date% %time%] Redefinindo estado de execução.
        set executed=0
    )
)

REM Aguarda 60 segundos antes de verificar novamente
timeout /t 60 /nobreak
goto loop

:run_script
echo [%date% %time%] Executando o script...

REM Ativa o ambiente Conda
echo [%date% %time%] Ativando o ambiente Conda...
CALL C:\Users\NBL\anaconda3\Library\bin\conda.bat activate Extracao || echo [%date% %time%] Erro ao ativar o Conda! Continuando...

REM Muda para o diretório do script Python
echo [%date% %time%] Mudando para o diretório do script Python...
cd /d C:\Users\NBL\Desktop\Automacoes NBL\automacao_455 || echo [%date% %time%] Erro ao mudar de diretório! Continuando...

REM Executa o script Python
echo [%date% %time%] Executando o script Python main.py...
python main.py || echo [%date% %time%] Erro ao executar o script Python! Continuando...

REM Marca como executado para evitar reexecução dentro do mesmo minuto
:mark_executed
set executed=1
echo [%date% %time%] Marcando estado de execução como concluído.

REM Aguarda 60 segundos antes de voltar ao loop
timeout /t 60 /nobreak
goto loop
