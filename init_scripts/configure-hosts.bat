@echo off
setlocal

:: ------------------------------------------------------------------ ::
:: Must run as Administrator                                           ::
:: ------------------------------------------------------------------ ::
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: This script must be run as Administrator.
    echo Right-click the file and select "Run as administrator".
    pause
    exit /b 1
)

:: ------------------------------------------------------------------ ::
:: Configuration                                                       ::
:: ------------------------------------------------------------------ ::
set HOSTS_FILE=C:\Windows\System32\drivers\etc\hosts
set WSL_CONF=/etc/wsl.conf

echo.
echo =^> Fetching Minikube IP from WSL...
for /f "delims=" %%i in ('wsl -e bash -c "minikube ip 2>/dev/null"') do set MINIKUBE_IP=%%i

if "%MINIKUBE_IP%"=="" (
    echo ERROR: Could not retrieve Minikube IP. Make sure Minikube is running in WSL.
    pause
    exit /b 1
)
echo    Minikube IP: %MINIKUBE_IP%

set HOSTS_ENTRY=%MINIKUBE_IP% kc.minikube.local minio.minikube.local minio-console.minikube.local

:: ------------------------------------------------------------------ ::
:: Add host entries if not already present                             ::
:: ------------------------------------------------------------------ ::
echo.
echo =^> Configuring Windows hosts file...

findstr /c:"minio.minikube.local" "%HOSTS_FILE%" >nul 2>&1
if %errorlevel% equ 0 (
    echo    Entries already present, skipping.
) else (
    echo %HOSTS_ENTRY% >> "%HOSTS_FILE%"
    echo    Added: %HOSTS_ENTRY%
)

:: ------------------------------------------------------------------ ::
:: Configure WSL to stop overwriting /etc/hosts on restart            ::
:: ------------------------------------------------------------------ ::
echo.
echo =^> Configuring WSL to preserve /etc/hosts...

wsl -e bash -c "grep -q 'generateHosts' /etc/wsl.conf 2>/dev/null && echo skip || (echo -e '\n[network]\ngenerateHosts=false' | sudo tee -a /etc/wsl.conf > /dev/null && echo done)"

echo.
echo =^> Done. Restart WSL for the wsl.conf change to take effect:
echo    wsl --shutdown
echo.
pause
endlocal
